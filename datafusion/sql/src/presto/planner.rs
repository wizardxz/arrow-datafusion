#![allow(non_snake_case)]
#![allow(dead_code)]

use antlr_rust::{
    errors::ANTLRError, parser_rule_context::ParserRuleContext, token::Token,
    token_factory::ArenaCommonFactory, tree::ParseTree,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use binder_macro::route;
use datafusion_expr::{
    col,
    expr::{self, Sort},
    lit,
    type_coercion::binary::comparison_coercion,
    utils::{
        expand_qualified_wildcard, expand_wildcard, expr_as_column_expr,
        find_aggregate_exprs, find_columns_referenced_by_expr, find_window_exprs,
    },
    AggregateFunction, Between, BinaryExpr, BuiltInWindowFunction, BuiltinScalarFunction,
    Case, Cast, CreateExternalTable, Expr, ExprSchemable, Filter, JoinType, LogicalPlan,
    LogicalPlanBuilder, Operator, Subquery, SubqueryAlias, TryCast, WindowFrame,
    WindowFrameBound, WindowFrameUnits, WindowFunction,
};
use std::{borrow::Cow, collections::hash_map::Entry::Occupied};

use super::{
    binding_context::{
        BindingContextStack, GroupbyExpressionContext, MultipleParentColumnBindingContext,
    },
    node::{
        CubeGroupingElement, GroupBy, GroupingElement, GroupingSet,
        MultipleGroupingElement, Properties, Property, Quantifier, RollupGroupingElement,
        SingleGroupingElement, WhenClause, WindowSpecification,
    },
};
use crate::{
    antlr::presto::{
        prestolexer::{DESC, EXCEPT, FIRST, INTERSECT, PLUS, PRECEDING, RANGE},
        prestoparser::*,
    },
    presto::binding_context::{CodeLocation, ColumnBindingContext, ValueContext},
    presto::parser::parse,
    presto::{binding_context::BindingContext, function::FunctionType},
    utils::{make_decimal_type, rebase_expr, resolve_columns},
};
use datafusion_common::{
    parse_interval, parsers::CompressionTypeVariant, BinderError, Column,
    DataFusionError, OwnedTableReference, Parser2Error, Result, ScalarValue,
    TableReference, ToDFSchema,
};
use datafusion_expr::Expr::Alias;
use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
    result,
    str::FromStr,
    sync::Arc,
};
trait Binder<S> {
    fn bind(&self, _: &BindingContextStack) -> Result<S> {
        Err(DataFusionError::NotImplemented(
            format_args!(
                "Not implemented {} for {}",
                std::any::type_name::<Self>(),
                std::any::type_name::<S>()
            )
            .to_string(),
        ))
    }
}

pub enum BindResult {
    Success(LogicalPlan),
    ANTLRError(ANTLRError),
    Parser2Error(Parser2Error),
    BindError(DataFusionError),
}

pub fn bind(sql: &str, bc: &BindingContextStack) -> BindResult {
    let tf = ArenaCommonFactory::default();
    let (root, error) = parse(sql, &tf);
    let parser_error_ref = error.borrow();
    match parser_error_ref.as_ref() {
        Some(e) => BindResult::Parser2Error(e.clone()),
        None => match root {
            Ok(root) => match root.bind(bc) {
                Ok(plan) => BindResult::Success(plan),
                Err(e) => BindResult::BindError(e),
            },
            Err(e) => BindResult::ANTLRError(e),
        },
    }
}

fn plan_from_tables<'input>(
    relations: Vec<Rc<RelationContextAll<'input>>>,
    bc: &BindingContextStack,
) -> Result<LogicalPlan> {
    match relations.len() {
        0 => LogicalPlanBuilder::empty(true).build(),
        1 => relations[0].bind(bc),
        _ => {
            let left = plan_from_tables(relations[0..relations.len() - 1].to_vec(), bc)?;
            let right = plan_from_tables(relations[relations.len() - 1..].to_vec(), bc)?;
            LogicalPlanBuilder::from(left).cross_join(right)?.build()
        }
    }
}

fn maybe_get_primary_expression<'input>(
    ctx: Rc<ExpressionContextAll<'input>>,
) -> Option<Rc<PrimaryExpressionContextAll<'input>>> {
    if let Some(ctx) = ctx.booleanExpression() {
        if let BooleanExpressionContextAll::PredicatedContext(ctx) = &*ctx {
            if ctx.predicate().is_some() {
                None
            } else {
                if let Some(ctx) = ctx.valueExpression() {
                    if let ValueExpressionContextAll::ValueExpressionDefaultContext(ctx) =
                        &*ctx
                    {
                        ctx.primaryExpression()
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    }
}

fn maybe_get_int<'input>(ctx: Rc<ExpressionContextAll<'input>>) -> Option<usize> {
    if let Some(ctx) = maybe_get_primary_expression(ctx) {
        if let PrimaryExpressionContextAll::NumericLiteralContext(ctx) = &*ctx {
            if let Some(ctx) = ctx.number() {
                if let NumberContextAll::IntegerLiteralContext(ctx) = &*ctx {
                    ctx.get_text().parse::<usize>().ok()
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}

fn maybe_get_string<'input>(ctx: Rc<ExpressionContextAll<'input>>) -> Option<String> {
    if let Some(ctx) = maybe_get_primary_expression(ctx) {
        maybe_get_string_from_PrimaryExpression(ctx)
    } else {
        None
    }
}

fn maybe_get_string_from_PrimaryExpression<'input>(
    ctx: Rc<PrimaryExpressionContextAll<'input>>,
) -> Option<String> {
    if let PrimaryExpressionContextAll::ColumnReferenceContext(ctx) = &*ctx {
        return Some(ctx.identifier().unwrap().get_text());
    } else {
        None
    }
}

/// Determines if the set of `Expr`'s are a valid projection on the input
/// `Expr::Column`'s.
fn check_columns_satisfy_exprs(
    columns: &[Expr],
    exprs: &[Expr],
    locations: &[CodeLocation],
) -> Result<()> {
    columns.iter().try_for_each(|c| match c {
        Expr::Column(_) => Ok(()),
        _ => Err(DataFusionError::Internal(
            "Expr::Column are required".to_string(),
        )),
    })?;
    let column_exprs = find_column_exprs(exprs, locations);

    for (e, location) in column_exprs.iter() {
        match e {
            Expr::GroupingSet(datafusion_expr::expr::GroupingSet::Rollup(exprs)) => {
                for e in exprs {
                    check_column_satisfies_expr(columns, e, location)?;
                }
            }
            Expr::GroupingSet(datafusion_expr::expr::GroupingSet::Cube(exprs)) => {
                for e in exprs {
                    check_column_satisfies_expr(columns, e, location)?;
                }
            }
            Expr::GroupingSet(datafusion_expr::expr::GroupingSet::GroupingSets(
                lists_of_exprs,
            )) => {
                for exprs in lists_of_exprs {
                    for e in exprs {
                        check_column_satisfies_expr(columns, e, location)?;
                    }
                }
            }
            _ => check_column_satisfies_expr(columns, e, location)?,
        }
    }
    Ok(())
}

/// Collect all deeply nested `Expr::Column`'s. They are returned in order of
/// appearance (depth first), and may contain duplicates.
fn find_column_exprs<'a>(
    exprs: &[Expr],
    locations: &'a [CodeLocation],
) -> Vec<(Expr, &'a CodeLocation)> {
    exprs
        .into_iter()
        .zip(locations.into_iter())
        .map(|(expr, location)| {
            find_columns_referenced_by_expr(expr)
                .into_iter()
                .map(|expr| (expr, location))
                .collect::<Vec<_>>()
        })
        .flatten()
        .map(|(expr, location)| (Expr::Column(expr), location))
        .collect()
}

fn check_column_satisfies_expr(
    columns: &[Expr],
    expr: &Expr,
    location: &CodeLocation,
) -> Result<()> {
    if !columns.contains(expr) {
        let available_columns = columns
            .iter()
            .map(|e| format!("{e}"))
            .collect::<Vec<String>>()
            .join(", ");
        return Err(DataFusionError::Bind(BinderError {
            row: location.row,
            col: location.col,
            message: format!(
                r#"Non-aggregate values are referenced: Expression {expr:?} could not be resolved from available columns: {available_columns}"#
            ),
        }));
    }
    Ok(())
}

fn bind_with_query(
    bc: &BindingContextStack,
    named_query_ctxes: &[Rc<NamedQueryContextAll>],
    query_no_with: &Rc<QueryNoWithContextAll>,
) -> Result<LogicalPlan> {
    if let Some((first, rest)) = named_query_ctxes.split_first() {
        let plan = first.bind(bc)?;
        let alias = match &plan {
            LogicalPlan::SubqueryAlias(plan) => Ok(plan.alias.clone()),
            LogicalPlan::Projection(plan) => {
                if let LogicalPlan::SubqueryAlias(plan) = &*plan.input {
                    Ok(plan.alias.clone())
                } else {
                    Err(DataFusionError::NotImplemented(format!(
                        "not implement namedQuery projection"
                    )))
                }
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "not implement namedQuery"
            ))),
        }?;
        if bc
            .resolve_table(
                &CodeLocation {
                    row: first.start().line,
                    col: first.start().column,
                },
                &TableReference::Bare {
                    table: Cow::from(alias.clone()),
                },
            )
            .is_ok()
        {
            return Err(DataFusionError::Bind(BinderError {
                row: first.start().line,
                col: first.start().column,
                message: format!("WITH query name {alias:?} specified more than once"),
            }));
        }

        struct TableBindingContext<'a> {
            plan: &'a LogicalPlan,
            alias: &'a String,
        }
        impl<'a> BindingContext for TableBindingContext<'a> {
            fn resolve_table(&self, table_ref: &TableReference) -> Result<LogicalPlan> {
                if let TableReference::Bare { table } = table_ref {
                    if table == self.alias {
                        Ok(self.plan.clone())
                    } else {
                        Err(DataFusionError::BindingContextInternal(Some(format!(
                            "Table {} does not exist",
                            table_ref.table()
                        ))))
                    }
                } else {
                    Err(DataFusionError::BindingContextInternal(Some(format!(
                        "Table {} does not exist",
                        table_ref.table()
                    ))))
                }
            }
        }
        bc.with_push(
            &TableBindingContext {
                plan: &plan,
                alias: &alias,
            },
            |bc| bind_with_query(bc, rest, query_no_with),
        )
    } else {
        query_no_with.bind(bc)
    }
}

// Begin generated boiler plate
impl Binder<LogicalPlan> for SingleStatementContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        self.statement().unwrap().as_ref().bind(bc)
    }
}

#[route(
    statementDefault,
    use,
    createSchema,
    dropSchema,
    renameSchema,
    setSchemaAuthorization,
    createTableAsSelect,
    createTable,
    dropTable,
    insertInto,
    delete,
    truncateTable,
    commentTable,
    commentView,
    commentColumn,
    renameTable,
    addColumn,
    renameColumn,
    dropColumn,
    setColumnType,
    setTableAuthorization,
    setTableProperties,
    tableExecute,
    analyze,
    createMaterializedView,
    createView,
    refreshMaterializedView,
    dropMaterializedView,
    renameMaterializedView,
    setMaterializedViewProperties,
    dropView,
    renameView,
    setViewAuthorization,
    call,
    createRole,
    dropRole,
    grantRoles,
    revokeRoles,
    setRole,
    grant,
    deny,
    revoke,
    showGrants,
    explain,
    explainAnalyze,
    showCreateTable,
    showCreateSchema,
    showCreateView,
    showCreateMaterializedView,
    showTables,
    showSchemas,
    showCatalogs,
    showColumns,
    showStats,
    showStatsForQuery,
    showRoles,
    showRoleGrants,
    showFunctions,
    showSession,
    setSession,
    resetSession,
    startTransaction,
    commit,
    rollback,
    prepare,
    deallocate,
    execute,
    describeInput,
    describeOutput,
    setPath,
    setTimeZone,
    update,
    merge
)]
impl Binder<LogicalPlan> for StatementContextAll<'_> {}
impl Binder<LogicalPlan> for StatementDefaultContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        self.query().unwrap().bind(bc)
    }
}

impl Binder<LogicalPlan> for UseContext<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<LogicalPlan> {
        todo!()
    }
}
impl Binder<LogicalPlan> for CreateSchemaContext<'_> {}
impl Binder<LogicalPlan> for DropSchemaContext<'_> {}
impl Binder<LogicalPlan> for RenameSchemaContext<'_> {}
impl Binder<LogicalPlan> for SetSchemaAuthorizationContext<'_> {}
impl Binder<LogicalPlan> for CreateTableAsSelectContext<'_> {}
impl Binder<LogicalPlan> for CreateTableContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        if self.COMMENT().is_some() {
            todo!()
        }
        let name = self.qualifiedName().unwrap().bind(bc)?;
        let schema = Schema::new(
            self.tableElement_all()
                .iter()
                .map(|ctx| ctx.bind(bc))
                .collect::<Result<Vec<_>>>()?,
        );
        let properties = if let Some(ctx) = self.properties() {
            Some(ctx.bind(bc)?)
        } else {
            None
        };
        match properties {
            Some(Properties {
                external_location: Some(external_location),
                format,
                compression,
                ..
            }) => {
                if (format.as_ref().map_or(false, |v| v != "CSV" && v != "JSON"))
                    && compression != CompressionTypeVariant::UNCOMPRESSED
                {
                    return Err(DataFusionError::Bind(BinderError {
                        row: self.properties().unwrap().start().line,
                        col: self.properties().unwrap().start().column,
                        message: format!(
                            "File compression type can be specified for CSV/JSON files."
                        ),
                    }));
                }
                if format.as_ref().map_or(false, |v| v == "PARQUET")
                    && schema.fields().len() > 0
                {
                    return Err(DataFusionError::Bind(BinderError {
                        row: self.tableElement(0).unwrap().start().line,
                        col: self.tableElement(0).unwrap().start().column,
                        message: format!(
                            "File compression type can be specified for CSV/JSON files."
                        ),
                    }));
                }

                Ok(LogicalPlan::CreateExternalTable(CreateExternalTable {
                    schema: schema.to_dfschema_ref()?,
                    name,
                    location: external_location,
                    file_type: format.unwrap_or(format!("CSV")),
                    has_header: false,
                    delimiter: ',',
                    table_partition_cols: vec![],
                    if_not_exists: false,
                    definition: None,
                    file_compression_type: compression,
                    options: HashMap::new(),
                }))
            }
            _ => todo!(),
        }
    }
}
impl Binder<LogicalPlan> for DropTableContext<'_> {}
impl Binder<LogicalPlan> for InsertIntoContext<'_> {}
impl Binder<LogicalPlan> for DeleteContext<'_> {}
impl Binder<LogicalPlan> for TruncateTableContext<'_> {}
impl Binder<LogicalPlan> for CommentTableContext<'_> {}
impl Binder<LogicalPlan> for CommentViewContext<'_> {}
impl Binder<LogicalPlan> for CommentColumnContext<'_> {}
impl Binder<LogicalPlan> for RenameTableContext<'_> {}
impl Binder<LogicalPlan> for AddColumnContext<'_> {}
impl Binder<LogicalPlan> for RenameColumnContext<'_> {}
impl Binder<LogicalPlan> for DropColumnContext<'_> {}
impl Binder<LogicalPlan> for SetColumnTypeContext<'_> {}
impl Binder<LogicalPlan> for SetTableAuthorizationContext<'_> {}
impl Binder<LogicalPlan> for SetTablePropertiesContext<'_> {}
impl Binder<LogicalPlan> for TableExecuteContext<'_> {}
impl Binder<LogicalPlan> for AnalyzeContext<'_> {}
impl Binder<LogicalPlan> for CreateMaterializedViewContext<'_> {}
impl Binder<LogicalPlan> for CreateViewContext<'_> {}
impl Binder<LogicalPlan> for RefreshMaterializedViewContext<'_> {}
impl Binder<LogicalPlan> for DropMaterializedViewContext<'_> {}
impl Binder<LogicalPlan> for RenameMaterializedViewContext<'_> {}
impl Binder<LogicalPlan> for SetMaterializedViewPropertiesContext<'_> {}
impl Binder<LogicalPlan> for DropViewContext<'_> {}
impl Binder<LogicalPlan> for RenameViewContext<'_> {}
impl Binder<LogicalPlan> for SetViewAuthorizationContext<'_> {}
impl Binder<LogicalPlan> for CallContext<'_> {}
impl Binder<LogicalPlan> for CreateRoleContext<'_> {}
impl Binder<LogicalPlan> for DropRoleContext<'_> {}
impl Binder<LogicalPlan> for GrantRolesContext<'_> {}
impl Binder<LogicalPlan> for RevokeRolesContext<'_> {}
impl Binder<LogicalPlan> for SetRoleContext<'_> {}
impl Binder<LogicalPlan> for GrantContext<'_> {}
impl Binder<LogicalPlan> for DenyContext<'_> {}
impl Binder<LogicalPlan> for RevokeContext<'_> {}
impl Binder<LogicalPlan> for ShowGrantsContext<'_> {}
impl Binder<LogicalPlan> for ExplainContext<'_> {}
impl Binder<LogicalPlan> for ExplainAnalyzeContext<'_> {}
impl Binder<LogicalPlan> for ShowCreateTableContext<'_> {}
impl Binder<LogicalPlan> for ShowCreateSchemaContext<'_> {}
impl Binder<LogicalPlan> for ShowCreateViewContext<'_> {}
impl Binder<LogicalPlan> for ShowCreateMaterializedViewContext<'_> {}
impl Binder<LogicalPlan> for ShowTablesContext<'_> {}
impl Binder<LogicalPlan> for ShowSchemasContext<'_> {}
impl Binder<LogicalPlan> for ShowCatalogsContext<'_> {}
impl Binder<LogicalPlan> for ShowColumnsContext<'_> {}
impl Binder<LogicalPlan> for ShowStatsContext<'_> {}
impl Binder<LogicalPlan> for ShowStatsForQueryContext<'_> {}
impl Binder<LogicalPlan> for ShowRolesContext<'_> {}
impl Binder<LogicalPlan> for ShowRoleGrantsContext<'_> {}
impl Binder<LogicalPlan> for ShowFunctionsContext<'_> {}
impl Binder<LogicalPlan> for ShowSessionContext<'_> {}
impl Binder<LogicalPlan> for SetSessionContext<'_> {}
impl Binder<LogicalPlan> for ResetSessionContext<'_> {}
impl Binder<LogicalPlan> for StartTransactionContext<'_> {}
impl Binder<LogicalPlan> for CommitContext<'_> {}
impl Binder<LogicalPlan> for RollbackContext<'_> {}
impl Binder<LogicalPlan> for PrepareContext<'_> {}
impl Binder<LogicalPlan> for DeallocateContext<'_> {}
impl Binder<LogicalPlan> for ExecuteContext<'_> {}
impl Binder<LogicalPlan> for DescribeInputContext<'_> {}
impl Binder<LogicalPlan> for DescribeOutputContext<'_> {}
impl Binder<LogicalPlan> for SetPathContext<'_> {}
impl Binder<LogicalPlan> for SetTimeZoneContext<'_> {}
impl Binder<LogicalPlan> for UpdateContext<'_> {}
impl Binder<LogicalPlan> for MergeContext<'_> {}
impl Binder<LogicalPlan> for QueryContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        if let Some(with_ctx) = self.with() {
            if with_ctx.RECURSIVE().is_some() {
                return Err(DataFusionError::NotImplemented(String::from(
                    "Recursive CTEs are not supported",
                )));
            }
        }
        let named_query_ctxes = if let Some(with_ctx) = self.with() {
            with_ctx.namedQuery_all()
        } else {
            vec![]
        };

        bind_with_query(bc, &named_query_ctxes, &self.queryNoWith().unwrap())
    }
}

impl Binder<Field> for TableElementContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Field> {
        if let Some(ctx) = self.columnDefinition() {
            ctx.bind(bc)
        } else {
            todo!()
        }
    }
}
impl Binder<Field> for ColumnDefinitionContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Field> {
        let data_type = self.type_().unwrap().bind(bc)?;
        if self.NULL().is_some() {
            todo!()
        }
        let allow_null = false;
        let name = self.identifier().unwrap().bind(bc)?;
        if self.COMMENT().is_some() {
            todo!()
        }
        if self.WITH().is_some() {
            todo!()
        }
        Ok(Field::new(name, data_type, allow_null))
    }
}
impl Binder<Properties> for PropertiesContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Properties> {
        self.propertyAssignments().unwrap().bind(bc)
    }
}
impl Binder<Properties> for PropertyAssignmentsContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Properties> {
        let mut result = Properties::new();
        let properties = self
            .property_all()
            .iter()
            .map(|ctx| ctx.bind(bc))
            .collect::<Result<Vec<_>>>()?;
        for property in properties.into_iter() {
            match property {
                Property {
                    ref key,
                    value: Expr::Literal(ScalarValue::Utf8(value)),
                } if key == "format" => result.format = value,
                Property {
                    ref key,
                    value: Expr::Literal(ScalarValue::Utf8(value)),
                } if key == "external_location" => result.external_location = value,
                Property {
                    ref key,
                    value: Expr::Literal(ScalarValue::Utf8(value)),
                } if key == "compression" => {
                    result.compression = match value.unwrap().as_str() {
                        "GZIP" => CompressionTypeVariant::GZIP,
                        "BZIP2" => CompressionTypeVariant::BZIP2,
                        _ => todo!(),
                    }
                }
                _ => todo!(),
            }
        }
        Ok(result)
    }
}
impl Binder<Property> for PropertyContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Property> {
        let key = self.identifier().unwrap().bind(bc)?.to_lowercase();
        let value = self.propertyValue().unwrap().bind(bc)?;
        Ok(Property { key, value })
    }
}
#[route(defaultPropertyValue, nonDefaultPropertyValue)]
impl Binder<Expr> for PropertyValueContextAll<'_> {}
impl Binder<Expr> for DefaultPropertyValueContext<'_> {}
impl Binder<Expr> for NonDefaultPropertyValueContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        self.expression().unwrap().bind(bc)
    }
}
impl Binder<LogicalPlan> for QueryNoWithContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        if self.FETCH().is_some() {
            todo!()
        }
        let plan = self.queryTerm().unwrap().as_ref().bind(bc)?;
        let items = &plan
            .schema()
            .fields()
            .iter()
            .map(|field| Expr::Column(field.qualified_column()))
            .collect::<Vec<_>>();
        let sort_exprs = bc.with_push(&ColumnBindingContext { parent: &plan }, |bc| {
            bc.with_push(&GroupbyExpressionContext { items: items }, |bc| {
                self.sortItem_all()
                    .iter()
                    .map(|ctx| ctx.bind(bc))
                    .collect::<Result<Vec<_>>>()
            })
        })?;
        let plan = if sort_exprs.is_empty() {
            Ok(plan)
        } else {
            LogicalPlanBuilder::from(plan).sort(sort_exprs)?.build()
        }?;

        let offset = if let Some(ctx) = self.offset.as_ref() {
            ctx.bind(bc)?
        } else {
            0
        };

        let limit = if let Some(ctx) = self.limit.as_ref() {
            if let Some(ctx) = ctx.rowCount() {
                Some(ctx.bind(bc)?)
            } else {
                None
            }
        } else {
            None
        };

        if offset > 0 || limit.is_some() {
            LogicalPlanBuilder::from(plan).limit(offset, limit)?.build()
        } else {
            Ok(plan)
        }
    }
}

impl Binder<usize> for RowCountContextAll<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<usize> {
        if self.QUESTION_MARK().is_some() {
            todo!()
        } else if let Some(ctx) = self.INTEGER_VALUE() {
            let text = ctx.get_text();
            text.parse().map_err(|_| {
                DataFusionError::Bind(BinderError {
                    row: self.start().line,
                    col: self.start().column,
                    message: format!("bad row count {text}"),
                })
            })
        } else {
            todo!()
        }
    }
}
#[route(queryTermDefault, setOperation)]
impl Binder<LogicalPlan> for QueryTermContextAll<'_> {}
impl Binder<LogicalPlan> for QueryTermDefaultContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        self.queryPrimary().unwrap().bind(bc)
    }
}

impl Binder<LogicalPlan> for SetOperationContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        let left = self.left.as_ref().unwrap().bind(bc)?;
        let right = self.right.as_ref().unwrap().bind(bc)?;
        let quantifier = if let Some(ctx) = self.setQuantifier().as_ref() {
            ctx.bind(bc)?
        } else {
            Quantifier::Distinct
        };
        let operator_ctx = self.operator.as_ref().unwrap();
        let left_col_num = left.schema().fields().len();

        // check union plan length same.
        let right_col_num = right.schema().fields().len();
        if right_col_num != left_col_num {
            return Err(DataFusionError::Bind(BinderError {
                row: operator_ctx.line,
                col: operator_ctx.column,
                message: format!(
                    "Union queries must have the same number of columns, \
                    (left is {left_col_num}, right is {right_col_num})"
                ),
            }));
        }
        (0..left_col_num)
            .map(|i| {
                let left_field = left.schema().field(i);
                let right_field = right.schema().field(i);
                comparison_coercion(left_field.data_type(), right_field.data_type())
                    .ok_or_else(|| {
                        DataFusionError::Bind(BinderError {
                            row: operator_ctx.line,
                            col: operator_ctx.column,
                            message: format!(
                                "UNION Column {} (type: {}) is not compatible \
                            with column {} (type: {})",
                                right_field.name(),
                                right_field.data_type(),
                                left_field.name(),
                                left_field.data_type()
                            ),
                        })
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        match (operator_ctx.get_token_type(), quantifier) {
            (INTERSECT, Quantifier::All) => {
                LogicalPlanBuilder::intersect(left, right, true)
            }
            (INTERSECT, Quantifier::Distinct) => {
                LogicalPlanBuilder::intersect(left, right, false)
            }
            (UNION, Quantifier::All) => {
                LogicalPlanBuilder::from(left).union(right)?.build()
            }
            (UNION, Quantifier::Distinct) => LogicalPlanBuilder::from(left)
                .union_distinct(right)?
                .build(),
            (EXCEPT, Quantifier::All) => LogicalPlanBuilder::except(left, right, true),
            (EXCEPT, Quantifier::Distinct) => {
                LogicalPlanBuilder::except(left, right, false)
            }
            _ => todo!(),
        }
    }
}
#[route(queryPrimaryDefault, table, inlineTable, subquery)]
impl Binder<LogicalPlan> for QueryPrimaryContextAll<'_> {}
impl Binder<LogicalPlan> for QueryPrimaryDefaultContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        self.querySpecification().unwrap().bind(bc)
    }
}

impl Binder<LogicalPlan> for TableContext<'_> {}
impl Binder<LogicalPlan> for InlineTableContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        let exprs = self
            .expression_all()
            .iter()
            .map(|ctx| {
                if let Some(ctx) = maybe_get_primary_expression(ctx.clone()) {
                    if let PrimaryExpressionContextAll::RowConstructorContext(ctx) = &*ctx
                    {
                        ctx.expression_all()
                            .iter()
                            .map(|ctx| ctx.bind(bc))
                            .collect::<Result<Vec<_>>>()
                    } else {
                        Ok(vec![ctx.bind(bc)?])
                    }
                } else {
                    Ok(vec![ctx.bind(bc)?])
                }
            })
            .collect::<Result<Vec<_>>>()?;
        LogicalPlanBuilder::values(exprs)?.build()
    }
}
impl Binder<LogicalPlan> for SubqueryContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        self.queryNoWith().unwrap().bind(bc)
    }
}
impl Binder<Expr> for SortItemContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let location = CodeLocation::new(self.start().line, self.start().column);
        let expr_ctx = self.expression().unwrap();
        let expr = match maybe_get_int(expr_ctx.clone()) {
            Some(ordinal) => bc.get_groupby_ordinal(&location, ordinal),
            None => expr_ctx.bind(bc),
        }?;

        let asc = if let Some(ordering) = self.ordering.as_ref() {
            ordering.get_token_type() != DESC
        } else {
            true
        };
        let nulls_first = if let Some(null_ordering) = self.nullOrdering.as_ref() {
            null_ordering.get_token_type() == FIRST
        } else {
            !asc
        };
        Ok(Expr::Sort(Sort::new(Box::new(expr), asc, nulls_first)))
    }
}
impl Binder<LogicalPlan> for QuerySpecificationContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        if self.setQuantifier().is_some() {
            todo!()
        }
        if self.windowDefinition_all().len() > 0 {
            todo!()
        }
        // bind from clause
        let parent = plan_from_tables(self.relation_all(), bc)?;

        // bind where clause
        let parent = match self.where_.as_ref() {
            Some(w) => {
                let predicate = bc
                    .with_push(&ColumnBindingContext { parent: &parent }, |bc| {
                        w.bind(bc)
                    })?;
                LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(parent))?)
            }
            None => parent,
        };

        // bind select items, expand wildcard
        let (select_items, select_item_ctxes) =
            bc.with_push(&ColumnBindingContext { parent: &parent }, |bc| {
                let mut seen = HashMap::<String, CodeLocation>::new();
                let mut exprs = Vec::<Expr>::new();
                let mut ctxes = Vec::<Rc<SelectItemContextAll>>::new();
                for ctx in self.querySelectItems().unwrap().selectItem_all() {
                    let expr = ctx.bind(bc)?;
                    let expanded_exprs = match expr {
                        Expr::Wildcard => expand_wildcard(parent.schema(), &parent),
                        Expr::QualifiedWildcard { qualifier } => {
                            expand_qualified_wildcard(qualifier.as_str(), parent.schema())
                        }
                        _ => Ok(vec![expr]),
                    }?;
                    for expr in expanded_exprs {
                        let name = expr.display_name()?;
                        let location =
                            CodeLocation::new(ctx.start().line, ctx.start().column);
                        if let Occupied(entry) = seen.entry(name.clone()) {
                            return Err(DataFusionError::Bind(BinderError {
                                row: location.row,
                                col: location.col,
                                message: format!(
                                    "Projections require unique expression \
                        names but the expression {} {} have the same name. \
                        Consider aliasing (\"AS\") one of them.",
                                    entry.get(),
                                    name
                                ),
                            }));
                        }
                        seen.insert(name, location);
                        exprs.push(expr);
                        ctxes.push(ctx.clone());
                    }
                }
                Ok((exprs, ctxes))
            })?;

        // bind having
        let having_expr = if let Some(ctx) = self.having.as_ref() {
            bc.with_push(&ColumnBindingContext { parent: &parent }, |bc| {
                bc.with_push(
                    &GroupbyExpressionContext {
                        items: &select_items,
                    },
                    |bc| Some(ctx.bind(bc)).transpose(),
                )
            })
        } else {
            Ok(None)
        }?;

        // The outer expressions we will search through for
        // aggregates. Aggregates may be sourced from the SELECT...
        let mut aggr_expr_haystack = select_items.clone();
        // ... or from the HAVING.
        if let Some(having_expr) = &having_expr {
            aggr_expr_haystack.push(having_expr.clone());
        }

        // All of the aggregate expressions (deduplicated).
        let aggr_exprs = find_aggregate_exprs(&aggr_expr_haystack);

        // bind group by expressions
        let group_by_exprs = if let Some(ctx) = self.groupBy() {
            let group_by =
                bc.with_push(&ColumnBindingContext { parent: &parent }, |bc| {
                    bc.with_push(
                        &GroupbyExpressionContext {
                            items: &select_items,
                        },
                        |bc| ctx.bind(bc),
                    )
                })?;
            group_by
                .grouping_elements
                .into_iter()
                .map(|g| Expr::from(g))
                .collect()
        } else {
            vec![]
        };

        // build group by aggregation if necessary
        let (parent, select_items, having_expr) = if !group_by_exprs.is_empty()
            || !aggr_exprs.is_empty()
        {
            let new_parent = LogicalPlanBuilder::from(parent.clone())
                .aggregate(group_by_exprs.clone(), aggr_exprs.clone())?
                .build()?;

            // in this next section of code we are re-writing the projection to refer to columns
            // output by the aggregate plan. For example, if the projection contains the expression
            // `SUM(a)` then we replace that with a reference to a column `SUM(a)` produced by
            // the aggregate plan.

            // combine the original grouping and aggregate expressions into one list (note that
            // we do not add the "having" expression since that is not part of the projection)
            let mut aggr_projection_exprs = vec![];

            for expr in &group_by_exprs {
                match expr {
                    Expr::GroupingSet(datafusion_expr::expr::GroupingSet::Rollup(
                        exprs,
                    )) => aggr_projection_exprs.extend_from_slice(exprs),
                    Expr::GroupingSet(datafusion_expr::expr::GroupingSet::Cube(
                        exprs,
                    )) => aggr_projection_exprs.extend_from_slice(exprs),
                    Expr::GroupingSet(
                        datafusion_expr::expr::GroupingSet::GroupingSets(lists_of_exprs),
                    ) => {
                        for exprs in lists_of_exprs {
                            aggr_projection_exprs.extend_from_slice(exprs)
                        }
                    }
                    _ => aggr_projection_exprs.push(expr.clone()),
                }
            }
            aggr_projection_exprs.extend_from_slice(&aggr_exprs);

            // now attempt to resolve columns and replace with fully-qualified columns
            let aggr_projection_exprs = aggr_projection_exprs
                .iter()
                .map(|expr| resolve_columns(expr, &parent))
                .collect::<Result<Vec<Expr>>>()?;

            // next we replace any expressions that are not a column with a column referencing
            // an output column from the aggregate schema
            let column_exprs_post_aggr = aggr_projection_exprs
                .iter()
                .map(|expr| expr_as_column_expr(expr, &parent))
                .collect::<Result<Vec<Expr>>>()?;

            // next we re-write the projection
            let select_exprs_post_aggr = select_items
                .iter()
                .map(|expr| rebase_expr(expr, &aggr_projection_exprs, &parent))
                .collect::<Result<Vec<Expr>>>()?;

            // finally, we have some validation that the re-written projection can be resolved
            // from the aggregate output columns
            check_columns_satisfy_exprs(
                &column_exprs_post_aggr,
                &select_exprs_post_aggr,
                &select_item_ctxes
                    .iter()
                    .map(|ctx| CodeLocation::new(ctx.start().line, ctx.start().column))
                    .collect::<Vec<_>>(),
            )?;

            // Rewrite the HAVING expression to use the columns produced by the
            // aggregation.
            let having_expr_post_aggr = if let Some(having_expr) = having_expr {
                let having_expr_post_aggr =
                    rebase_expr(&having_expr, &aggr_projection_exprs, &parent)?;

                let ctx = self.having.as_ref().unwrap();
                check_columns_satisfy_exprs(
                    &column_exprs_post_aggr,
                    &[having_expr_post_aggr.clone()],
                    &[CodeLocation::new(ctx.start().line, ctx.start().column)],
                )?;

                Some(having_expr_post_aggr)
            } else {
                None
            };
            Ok((new_parent, select_exprs_post_aggr, having_expr_post_aggr))
        } else {
            match having_expr {
                Some(having_expr) => {
                    let ctx = self.having.as_ref().unwrap();
                    Err(DataFusionError::Bind(BinderError{ row: ctx.start().line, col: ctx.start().column, message: format!("HAVING clause references: {having_expr} must appear in the GROUP BY clause or be used in an aggregate function") }))
                }
                None => Ok((parent, select_items, having_expr)),
            }
        }?;

        // bind having expressions and build the parent if having clause exists
        let parent = if let Some(having_expr) = having_expr {
            LogicalPlanBuilder::from(parent)
                .filter(having_expr)?
                .build()?
        } else {
            parent
        };

        // process window function
        let window_func_exprs = find_window_exprs(&select_items);

        let (parent, select_items) = if window_func_exprs.is_empty() {
            (parent, select_items)
        } else {
            let parent =
                LogicalPlanBuilder::window_plan(parent, window_func_exprs.clone())?;

            // re-write the projection
            let select_items = select_items
                .iter()
                .map(|expr| rebase_expr(expr, &window_func_exprs, &parent))
                .collect::<Result<Vec<Expr>>>()?;

            (parent, select_items)
        };

        // build the projection
        LogicalPlanBuilder::from(parent)
            .project(select_items)?
            .build()
    }
}

impl Binder<GroupBy> for GroupByContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<GroupBy> {
        let quantifier = if let Some(ctx) = self.setQuantifier().as_ref() {
            ctx.bind(bc)?
        } else {
            Quantifier::Distinct
        };
        let grouping_elements = self
            .groupingElement_all()
            .iter()
            .map(|ctx| ctx.bind(bc))
            .collect::<Result<_>>()?;
        Ok(GroupBy {
            quantifier,
            grouping_elements,
        })
    }
}
#[route(singleGroupingSet, rollup, cube, multipleGroupingSets)]
impl Binder<GroupingElement> for GroupingElementContextAll<'_> {}
impl Binder<GroupingElement> for SingleGroupingSetContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<GroupingElement> {
        let grouping_set = self.groupingSet().unwrap().bind(bc)?;
        if grouping_set.exprs.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "single grouping set should only contain 1 expr"
            )));
        }
        Ok(GroupingElement::Single(SingleGroupingElement {
            expr: grouping_set.exprs.into_iter().next().unwrap(),
        }))
    }
}
impl Binder<GroupingElement> for RollupContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<GroupingElement> {
        let exprs = self
            .expression_all()
            .iter()
            .map(|expr| expr.bind(bc))
            .collect::<Result<_>>()?;
        Ok(GroupingElement::Rollup(RollupGroupingElement { exprs }))
    }
}
impl Binder<GroupingElement> for CubeContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<GroupingElement> {
        let exprs = self
            .expression_all()
            .iter()
            .map(|expr| expr.bind(bc))
            .collect::<Result<_>>()?;
        Ok(GroupingElement::Cube(CubeGroupingElement { exprs }))
    }
}
impl Binder<GroupingElement> for MultipleGroupingSetsContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<GroupingElement> {
        let grouping_sets = self
            .groupingSet_all()
            .iter()
            .map(|g| g.bind(bc))
            .collect::<Result<Vec<GroupingSet>>>()?;
        Ok(GroupingElement::Multiple(MultipleGroupingElement {
            grouping_sets,
        }))
    }
}
impl Binder<GroupingSet> for GroupingSetContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<GroupingSet> {
        let exprs = self
            .expression_all()
            .iter()
            .map(|ctx| {
                let location = CodeLocation::new(ctx.start().line, ctx.start().column);
                match maybe_get_int(ctx.clone()) {
                    Some(ordinal) => bc.get_groupby_ordinal(&location, ordinal),
                    None => ctx.bind(bc),
                }
            })
            .collect::<Result<_>>()?;
        Ok(GroupingSet { exprs })
    }
}
impl Binder<WindowSpecification> for WindowSpecificationContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<WindowSpecification> {
        if self.existingWindowName.as_ref().is_some() {
            todo!();
        }
        let partitions = self
            .partition
            .iter()
            .map(|ctx| ctx.bind(bc))
            .collect::<Result<Vec<_>>>()?;
        let sort_items = self
            .sortItem_all()
            .iter()
            .map(|ctx| ctx.bind(bc))
            .collect::<Result<Vec<_>>>()?;
        let window_frame = if let Some(ctx) = self.windowFrame() {
            let window_frame = ctx.bind(bc)?;
            if WindowFrameUnits::Range == window_frame.units && sort_items.len() != 1 {
                Err(DataFusionError::Bind(BinderError {
                    row: self.start().line,
                    col: self.start().column,
                    message: format!(
                        "With window frame of type RANGE, the order by expression must be of length 1, got {}", sort_items.len()),
                }))
            } else {
                Ok(window_frame)
            }
        } else {
            Ok(WindowFrame::new(!sort_items.is_empty()))
        }?;

        Ok(WindowSpecification {
            partitions,
            sort_items,
            window_frame,
        })
    }
}
impl Binder<LogicalPlan> for NamedQueryContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        let plan = self.query().unwrap().bind(bc)?;
        let name = self.name.as_ref().unwrap().bind(bc)?;
        let plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(plan, name)?);

        if let Some(ctx) = self.columnAliases() {
            let idents = ctx.bind(bc)?;
            if idents.len() != plan.schema().fields().len() {
                return Err(DataFusionError::Bind(BinderError {
                    row: ctx.start().line,
                    col: ctx.start().column,
                    message: format!(
                        "Source table contains {} columns but only {} names given as column alias",
                        plan.schema().fields().len(),
                        idents.len(),
                    ),
                }));
            }
            let fields = plan.schema().fields().clone();
            LogicalPlanBuilder::from(plan)
                .project(
                    fields
                        .iter()
                        .zip(idents.into_iter())
                        .map(|(field, ident)| col(field.name()).alias(ident)),
                )?
                .build()
        } else {
            Ok(plan)
        }
    }
}
impl Binder<Quantifier> for SetQuantifierContextAll<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<Quantifier> {
        if self.ALL().is_some() {
            Ok(Quantifier::All)
        } else {
            Ok(Quantifier::Distinct)
        }
    }
}
#[route(selectSingle, selectAll)]
impl Binder<Expr> for SelectItemContextAll<'_> {}
impl Binder<Expr> for SelectSingleContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let expr = self.expression().unwrap().bind(bc)?;

        match self.identifier() {
            Some(identifier) => {
                let alias = identifier.bind(bc)?;
                Ok(Alias(Box::new(expr), alias))
            }
            None => Ok(expr),
        }
    }
}

impl Binder<Expr> for SelectAllContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        if self.columnAliases().is_some() {
            todo!()
        }
        if self.primaryExpression().is_none() {
            return Ok(Expr::Wildcard);
        }
        match &*self.primaryExpression().unwrap() {
            PrimaryExpressionContextAll::ColumnReferenceContext(c) => {
                let qualifier = c.identifier().unwrap().bind(bc)?;
                Ok(Expr::QualifiedWildcard {
                    qualifier: qualifier,
                })
            }
            _ => todo!(),
        }
    }
}
#[route(joinRelation, relationDefault)]
impl Binder<LogicalPlan> for RelationContextAll<'_> {}
impl Binder<LogicalPlan> for JoinRelationContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        let left = self.left.as_ref().unwrap().bind(bc)?;
        if self.CROSS().is_some() {
            let right = self.right.as_ref().unwrap().bind(bc)?;
            LogicalPlanBuilder::from(left).cross_join(right)?.build()
        } else if self.NATURAL().is_some() {
            let right = self.right.as_ref().unwrap().bind(bc)?;

            let join_type = self.joinType().unwrap().bind(bc)?;
            let left_cols: HashSet<&String> = left
                .schema()
                .fields()
                .iter()
                .map(|f| f.field().name())
                .collect();
            let keys: Vec<Column> = right
                .schema()
                .fields()
                .iter()
                .map(|f| f.field().name())
                .filter(|f| left_cols.contains(f))
                .map(Column::from_name)
                .collect();
            if keys.is_empty() {
                LogicalPlanBuilder::from(left).cross_join(right)?.build()
            } else {
                LogicalPlanBuilder::from(left)
                    .join_using(right, join_type, keys)?
                    .build()
            }
        } else {
            let right = self.rightRelation.as_ref().unwrap().bind(bc)?;
            let identifiers = self.joinCriteria().unwrap().identifier_all();
            if self.joinCriteria().unwrap().USING().is_some() {
                let find_columns = |parent| {
                    bc.with_push(&ColumnBindingContext { parent: parent }, |bc| {
                        identifiers
                            .iter()
                            .map(|identifier_ctx| {
                                let name = identifier_ctx.bind(bc)?;
                                match bc.resolve_column(
                                    &CodeLocation::new(
                                        identifier_ctx.start().line,
                                        identifier_ctx.start().column,
                                    ),
                                    &name,
                                ) {
                                    Ok(Expr::Column(column)) => Ok(column),
                                    Ok(_) => todo!(),
                                    Err(e) => Err(e),
                                }
                            })
                            .collect::<Result<Vec<Column>>>()
                    })
                };
                let _ = find_columns(&left)?;
                let _ = find_columns(&right)?;
                let join_type = self.joinType().unwrap().bind(bc)?;
                let keys = identifiers
                    .iter()
                    .map(|identifier_ctx| {
                        let name = identifier_ctx.bind(bc)?;
                        Ok(Column {
                            relation: None,
                            name: name,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                LogicalPlanBuilder::from(left)
                    .join_using(right, join_type, keys)?
                    .build()
            } else if self.joinCriteria().unwrap().ON().is_some() {
                let on_expr = bc.with_push(
                    &MultipleParentColumnBindingContext {
                        parents: vec![&left, &right],
                    },
                    |bc| {
                        self.joinCriteria()
                            .unwrap()
                            .booleanExpression()
                            .unwrap()
                            .bind(bc)
                    },
                )?;
                let join_type = self.joinType().unwrap().bind(bc)?;
                LogicalPlanBuilder::from(left)
                    .join(
                        right,
                        join_type,
                        (Vec::<Column>::new(), Vec::<Column>::new()),
                        Some(on_expr),
                    )?
                    .build()
            } else {
                todo!()
            }
        }
    }
}
impl Binder<LogicalPlan> for RelationDefaultContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        self.sampledRelation().unwrap().as_ref().bind(bc)
    }
}

impl Binder<JoinType> for JoinTypeContextAll<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<JoinType> {
        if self.INNER().is_some() {
            return Ok(JoinType::Inner);
        } else if self.LEFT().is_some() {
            return Ok(JoinType::Left);
        } else if self.RIGHT().is_some() {
            return Ok(JoinType::Right);
        } else if self.FULL().is_some() {
            return Ok(JoinType::Full);
        } else {
            return Ok(JoinType::Inner);
        }
    }
}
impl Binder<LogicalPlan> for SampledRelationContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        if self.sampleType().is_some() {
            todo!()
        }
        self.patternRecognition().unwrap().as_ref().bind(bc)
    }
}

impl Binder<LogicalPlan> for PatternRecognitionContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        if self.MATCH_RECOGNIZE().is_some() {
            todo!()
        }
        self.aliasedRelation().unwrap().as_ref().bind(bc)
    }
}

impl Binder<LogicalPlan> for AliasedRelationContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        let relation = self.relationPrimary().unwrap().as_ref().bind(bc)?;
        match self.identifier() {
            Some(identifier) => {
                let alias = identifier.bind(bc)?;
                let relation = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                    relation,
                    alias.clone(),
                )?);
                match self.columnAliases() {
                    Some(column_aliases_ctx) => {
                        let column_aliases = column_aliases_ctx.bind(bc)?;
                        if column_aliases.len() != relation.schema().fields().len() {
                            return Err(DataFusionError::Bind(BinderError {
                                row: column_aliases_ctx.start().line,
                                col: column_aliases_ctx.start().column,
                                message: format!("Source table contains {} columns but only {} names given as column alias", relation.schema().fields().len(), column_aliases.len()),
                            }));
                        }
                        let items: Vec<Expr> = relation
                            .schema()
                            .fields()
                            .iter()
                            .zip(column_aliases.into_iter())
                            .map(|(field, column_alias)| {
                                Alias(
                                    Box::new(Expr::Column(Column {
                                        relation: Some(alias.clone()),
                                        name: field.name().clone(),
                                    })),
                                    column_alias,
                                )
                            })
                            .collect();
                        LogicalPlanBuilder::from(relation).project(items)?.build()
                    }
                    None => Ok(relation),
                }
            }
            None => Ok(relation),
        }
    }
}

impl Binder<Vec<String>> for ColumnAliasesContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Vec<String>> {
        self.identifier_all()
            .iter()
            .map(|identifier| identifier.bind(bc))
            .collect()
    }
}
#[route(
    tableName,
    subqueryRelation,
    unnest,
    lateral,
    tableFunctionInvocation,
    parenthesizedRelation
)]
impl Binder<LogicalPlan> for RelationPrimaryContextAll<'_> {}
impl Binder<LogicalPlan> for TableNameContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        if self.queryPeriod().is_some() {
            todo!()
        }

        let table_ref_result: OwnedTableReference =
            self.qualifiedName().unwrap().as_ref().bind(bc)?;
        let table_ref = table_ref_result.as_table_reference();
        bc.resolve_table(
            &CodeLocation::new(self.start().line, self.start().column),
            &table_ref,
        )
    }
}

impl Binder<LogicalPlan> for SubqueryRelationContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        self.query().unwrap().bind(bc)
    }
}
impl Binder<LogicalPlan> for UnnestContext<'_> {}
impl Binder<LogicalPlan> for LateralContext<'_> {}
impl Binder<LogicalPlan> for TableFunctionInvocationContext<'_> {}
impl Binder<LogicalPlan> for ParenthesizedRelationContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<LogicalPlan> {
        self.relation().unwrap().bind(bc)
    }
}
impl Binder<Expr> for ExpressionContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        self.booleanExpression().unwrap().as_ref().bind(bc)
    }
}

#[route(predicated, logicalNot, and, or)]
impl Binder<Expr> for BooleanExpressionContextAll<'_> {}
impl Binder<Expr> for PredicatedContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let value = self.valueExpression().unwrap().bind(bc)?;
        match self.predicate() {
            Some(predicate) => {
                bc.with_push(&ValueContext { value: &value }, |bc| predicate.bind(bc))
            }
            None => Ok(value),
        }
    }
}

impl Binder<Expr> for LogicalNotContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let expr = self.booleanExpression().unwrap().bind(bc)?;
        Ok(Expr::Not(Box::new(expr)))
    }
}
impl Binder<Expr> for AndContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let left = self.booleanExpression(0).unwrap().bind(bc)?;
        let right = self.booleanExpression(1).unwrap().bind(bc)?;
        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::And,
            Box::new(right),
        )))
    }
}

impl Binder<Expr> for OrContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let left = self.booleanExpression(0).unwrap().bind(bc)?;
        let right = self.booleanExpression(1).unwrap().bind(bc)?;
        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::Or,
            Box::new(right),
        )))
    }
}
#[route(
    comparison,
    quantifiedComparison,
    between,
    inList,
    inSubquery,
    like,
    nullPredicate,
    distinctFrom
)]
impl Binder<Expr> for PredicateContextAll<'_> {}
impl Binder<Expr> for ComparisonContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let left =
            bc.get_value(&CodeLocation::new(self.start().line, self.start().column))?;
        let right = self.right.as_ref().unwrap().bind(bc)?;
        let operator_ctx = self.comparisonOperator().unwrap();
        let operator = if operator_ctx.EQ().is_some() {
            Ok(Operator::Eq)
        } else if operator_ctx.NEQ().is_some() {
            Ok(Operator::NotEq)
        } else if operator_ctx.LT().is_some() {
            Ok(Operator::Lt)
        } else if operator_ctx.LTE().is_some() {
            Ok(Operator::LtEq)
        } else if operator_ctx.GT().is_some() {
            Ok(Operator::Gt)
        } else if operator_ctx.GTE().is_some() {
            Ok(Operator::GtEq)
        } else {
            Err(DataFusionError::NotImplemented(format!(
                "not implemented operator {}",
                operator_ctx.get_text()
            )))
        }?;
        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            operator,
            Box::new(right),
        )))
    }
}
impl Binder<Expr> for QuantifiedComparisonContext<'_> {}
impl Binder<Expr> for BetweenContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let left =
            bc.get_value(&CodeLocation::new(self.start().line, self.start().column))?;
        let lower = self.lower.as_ref().unwrap().bind(bc)?;
        let upper = self.upper.as_ref().unwrap().bind(bc)?;
        let negated = self.NOT().is_some();
        Ok(Expr::Between(Between::new(
            Box::new(left),
            negated,
            Box::new(lower),
            Box::new(upper),
        )))
    }
}
impl Binder<Expr> for InListContext<'_> {}
impl Binder<Expr> for InSubqueryContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let left =
            bc.get_value(&CodeLocation::new(self.start().line, self.start().column))?;
        let plan = self.query().unwrap().bind(bc)?;
        let negated = self.NOT().is_some();
        Ok(Expr::InSubquery {
            expr: Box::new(left),
            subquery: Subquery {
                subquery: Arc::new(plan),
            },
            negated,
        })
    }
}
impl Binder<Expr> for LikeContext<'_> {}
impl Binder<Expr> for NullPredicateContext<'_> {}
impl Binder<Expr> for DistinctFromContext<'_> {}
#[route(
    valueExpressionDefault,
    atTimeZone,
    arithmeticUnary,
    arithmeticBinary,
    concatenation
)]
impl Binder<Expr> for ValueExpressionContextAll<'_> {}
impl Binder<Expr> for ValueExpressionDefaultContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        self.primaryExpression().unwrap().bind(bc)
    }
}

impl Binder<Expr> for AtTimeZoneContext<'_> {}
impl Binder<Expr> for ArithmeticUnaryContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let expr = self.valueExpression().unwrap().bind(bc)?;

        if self.MINUS().is_some() {
            Ok(Expr::Negative(Box::new(expr)))
        } else {
            Ok(expr)
        }
    }
}
impl Binder<Expr> for ArithmeticBinaryContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let left = self.left.as_ref().unwrap().bind(bc)?;
        let right = self.right.as_ref().unwrap().bind(bc)?;
        let operator_ctx = self.operator.unwrap();
        let operator = match operator_ctx.get_token_type() {
            PLUS => Ok(Operator::Plus),
            MINUS => Ok(Operator::Minus),
            ASTERISK => Ok(Operator::Multiply),
            SLASH => Ok(Operator::Divide),
            PERCENT => Ok(Operator::Modulo),
            _ => Err(DataFusionError::Parser(Parser2Error {
                row: operator_ctx.line,
                col: operator_ctx.column,
                message: format!("Invalid binary operator {}", operator_ctx.get_text()),
            })),
        }?;

        Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            operator,
            Box::new(right),
        )))
    }
}
impl Binder<Expr> for ConcatenationContext<'_> {}
#[route(
    nullLiteral,
    intervalLiteral,
    typeConstructor,
    numericLiteral,
    booleanLiteral,
    stringLiteral,
    binaryLiteral,
    parameter,
    position,
    rowConstructor,
    listagg,
    functionCall,
    measure,
    lambda,
    subqueryExpression,
    exists,
    simpleCase,
    searchedCase,
    cast,
    arrayConstructor,
    subscript,
    columnReference,
    dereference,
    specialDateTimeFunction,
    currentUser,
    currentCatalog,
    currentSchema,
    currentPath,
    trim,
    substring,
    normalize,
    extract,
    parenthesizedExpression,
    groupingOperation,
    jsonExists,
    jsonValue,
    jsonQuery,
    jsonObject,
    jsonArray
)]
impl Binder<Expr> for PrimaryExpressionContextAll<'_> {}
impl Binder<Expr> for NullLiteralContext<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<Expr> {
        Ok(Expr::Literal(ScalarValue::Null))
    }
}
impl Binder<Expr> for IntervalLiteralContext<'_> {}
impl Binder<Expr> for TypeConstructorContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let identifier = self.identifier().unwrap().bind(bc)?;
        let value = self.string().unwrap().bind(bc)?;
        match identifier.to_lowercase().as_str() {
            "date" => Ok(Expr::Cast(Cast::new(
                Box::new(lit(value)),
                DataType::Date32,
            ))),
            "timestamp" => Ok(Expr::Cast(Cast::new(
                Box::new(lit(value)),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ))),
            "interval" => match parse_interval("", &value) {
                Ok(interval) => Ok(lit(interval)),
                Err(e) => Err(DataFusionError::Parser(Parser2Error {
                    row: self.start().line,
                    col: self.start().column,
                    message: e.to_string(),
                })),
            },
            "time" => Ok(Expr::Cast(Cast::new(
                Box::new(lit(value)),
                DataType::Time64(TimeUnit::Nanosecond),
            ))),
            _ => todo!(),
        }
    }
}
impl Binder<Expr> for NumericLiteralContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        self.number().unwrap().bind(bc)
    }
}
impl Binder<Expr> for BooleanLiteralContext<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<Expr> {
        if self.booleanValue().unwrap().TRUE().is_some() {
            Ok(lit(true))
        } else if self.booleanValue().unwrap().FALSE().is_some() {
            Ok(lit(false))
        } else {
            Err(DataFusionError::Parser(Parser2Error {
                row: self.start().line,
                col: self.start().column,
                message: format!("invalid boolean value"),
            }))
        }
    }
}
impl Binder<Expr> for StringLiteralContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        Ok(lit(self.string().unwrap().bind(bc)?))
    }
}
impl Binder<Expr> for BinaryLiteralContext<'_> {}
impl Binder<Expr> for ParameterContext<'_> {}
impl Binder<Expr> for PositionContext<'_> {}
impl Binder<Expr> for RowConstructorContext<'_> {}
impl Binder<Expr> for ListaggContext<'_> {}
impl Binder<Expr> for FunctionCallContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        if self.processingMode().is_some() {
            todo!()
        }
        if self.setQuantifier().is_some() {
            todo!()
        }
        if self.sortItem_all().len() > 0 {
            todo!()
        }

        let qualified_name = self.qualifiedName().unwrap();
        let identifiers: Vec<_> = qualified_name.bind(bc)?;
        if identifiers.len() != 1 {
            todo!()
        }

        let name = &identifiers[0];
        let arguments = if name.to_lowercase() == "count" && self.ASTERISK().is_some() {
            vec![lit::<u8>(1)]
        } else {
            self.expression_all()
                .iter()
                .map(|expr| expr.bind(bc))
                .collect::<result::Result<Vec<_>, _>>()?
        };
        let argument_types: Vec<_> = arguments
            .iter()
            .map(|arg| {
                arg.get_type(
                    &bc.get_schema(&CodeLocation::new(
                        self.start().line,
                        self.start().column,
                    ))
                    .unwrap(),
                )
                .unwrap()
            })
            .collect();

        let window_specification = if let Some(o) = self.over() {
            Some(o.bind(bc)?)
        } else {
            None
        };

        let function_overloads = bc.resolve_function(
            &CodeLocation::new(
                qualified_name.start().line,
                qualified_name.start().column,
            ),
            &name,
            &argument_types,
        )?;

        let filter = if let Some(filter) = self.filter() {
            Some(Box::new(filter.bind(bc)?))
        } else {
            None
        };

        match function_overloads.type_ {
            FunctionType::Scalar => {
                let fun = BuiltinScalarFunction::from_str(&name.to_lowercase())?;
                Ok(Expr::ScalarFunction {
                    fun: fun,
                    args: arguments,
                })
            }
            FunctionType::Aggregate => {
                let fun = AggregateFunction::from_str(&name.to_lowercase())?;
                if let Some(window_specification) = window_specification {
                    Ok(Expr::WindowFunction(expr::WindowFunction::new(
                        WindowFunction::AggregateFunction(fun),
                        arguments,
                        window_specification.partitions,
                        window_specification.sort_items,
                        window_specification.window_frame,
                    )))
                } else {
                    Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                        fun, arguments, false, filter,
                    )))
                }
            }
            FunctionType::Window => {
                let fun = BuiltInWindowFunction::from_str(&name.to_lowercase())?;
                if let Some(window_specification) = window_specification {
                    Ok(Expr::WindowFunction(expr::WindowFunction::new(
                        WindowFunction::BuiltInWindowFunction(fun),
                        arguments,
                        window_specification.partitions,
                        window_specification.sort_items,
                        window_specification.window_frame,
                    )))
                } else {
                    todo!()
                }
            }
        }
    }
}
impl Binder<Expr> for MeasureContext<'_> {}
impl Binder<Expr> for LambdaContext<'_> {}
impl Binder<Expr> for SubqueryExpressionContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let plan = self.query().unwrap().bind(bc)?;
        Ok(Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(plan),
        }))
    }
}
impl Binder<Expr> for ExistsContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let plan = self.query().unwrap().bind(bc)?;
        Ok(Expr::Exists {
            subquery: Subquery {
                subquery: Arc::new(plan),
            },
            negated: false,
        })
    }
}
impl Binder<Expr> for SimpleCaseContext<'_> {}
impl Binder<Expr> for SearchedCaseContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let when_clauses = self
            .whenClause_all()
            .iter()
            .map(|ctx| ctx.bind(bc))
            .collect::<Result<Vec<_>>>()?;
        let else_expr = if let Some(ctx) = self.elseExpression.as_ref() {
            Some(Box::new(ctx.bind(bc)?))
        } else {
            None
        };

        Ok(Expr::Case(Case::new(
            None,
            when_clauses
                .into_iter()
                .map(|when_clause| {
                    (
                        Box::new(when_clause.condition),
                        Box::new(when_clause.result),
                    )
                })
                .collect::<Vec<_>>(),
            else_expr,
        )))
    }
}
impl Binder<Expr> for CastContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let expr = self.expression().unwrap().bind(bc)?;
        let type_ = self.type_().unwrap().bind(bc)?;
        if self.CAST().is_some() {
            Ok(Expr::Cast(Cast::new(Box::new(expr), type_)))
        } else if self.TRY_CAST().is_some() {
            Ok(Expr::TryCast(TryCast::new(Box::new(expr), type_)))
        } else {
            todo!()
        }
    }
}
impl Binder<Expr> for ArrayConstructorContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let mut child_type: Option<DataType> = None;
        let items = self
            .expression_all()
            .iter()
            .map(|ctx| {
                let expr = ctx.bind(bc)?;
                if let Expr::Literal(l) = expr {
                    let dt = l.get_datatype();
                    if child_type.is_none() {
                        child_type = Some(dt.clone());
                    } else if child_type != Some(dt.clone()) {
                        return Err(DataFusionError::Bind(BinderError {
                            row: ctx.start().line,
                            col: ctx.start().column,
                            message: format!("Arrays with different types are not supported: {:?} != {:?}", dt.clone(), child_type),
                        }));
                    }
                    Ok(l)
                } else {
                    Err(DataFusionError::Bind(BinderError {
                        row: ctx.start().line,
                        col: ctx.start().column,
                        message: format!("Arrays with elements other than literal are not supported"),
                    }))
                }
            })
            .collect::<Result<_>>()?;
        Ok(lit(ScalarValue::new_list(Some(items), child_type.unwrap())))
    }
}
impl Binder<Expr> for SubscriptContext<'_> {}
impl Binder<Expr> for ColumnReferenceContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let name = self.identifier().unwrap().bind(bc)?;
        let location = CodeLocation::new(self.start().line, self.start().column);
        match bc.resolve_column(
            &CodeLocation::new(self.start().line, self.start().column),
            name.as_str(),
        ) {
            Ok(expr) => Ok(expr),
            Err(e) => {
                if let Ok(alias) = self.identifier().unwrap().bind(bc) {
                    match bc.get_groupby_alias(&location, alias.as_str()) {
                        Ok(expr) => Ok(expr),
                        _ => Err(e),
                    }
                } else {
                    Err(e)
                }
            }
        }
    }
}

impl Binder<Expr> for DereferenceContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        if let Some(base) =
            maybe_get_string_from_PrimaryExpression(self.base_.clone().unwrap())
        {
            let field_name = self.identifier().unwrap().bind(bc)?;
            return bc.resolve_qualified_column(
                &CodeLocation::new(self.start().line, self.start().column),
                base.as_str(),
                field_name.as_str(),
            );
        }
        Err(DataFusionError::Bind(BinderError {
            row: self.start().line,
            col: self.start().column,
            message: format!("failed to bind dereference"),
        }))
    }
}
impl Binder<Expr> for SpecialDateTimeFunctionContext<'_> {}
impl Binder<Expr> for CurrentUserContext<'_> {}
impl Binder<Expr> for CurrentCatalogContext<'_> {}
impl Binder<Expr> for CurrentSchemaContext<'_> {}
impl Binder<Expr> for CurrentPathContext<'_> {}
impl Binder<Expr> for TrimContext<'_> {}
impl Binder<Expr> for SubstringContext<'_> {}
impl Binder<Expr> for NormalizeContext<'_> {}
impl Binder<Expr> for ExtractContext<'_> {}
impl Binder<Expr> for ParenthesizedExpressionContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        self.expression().unwrap().bind(bc)
    }
}
impl Binder<Expr> for GroupingOperationContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let arguments = self
            .qualifiedName_all()
            .iter()
            .map(|ctx| ctx.bind(bc))
            .collect::<Result<Vec<Expr>>>()?;
        Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
            AggregateFunction::Grouping,
            arguments,
            false,
            None,
        )))
    }
}
impl Binder<Expr> for JsonExistsContext<'_> {}
impl Binder<Expr> for JsonValueContext<'_> {}
impl Binder<Expr> for JsonQueryContext<'_> {}
impl Binder<Expr> for JsonObjectContext<'_> {}
impl Binder<Expr> for JsonArrayContext<'_> {}
#[route(basicStringLiteral, unicodeStringLiteral)]
impl Binder<String> for StringContextAll<'_> {}
impl Binder<String> for BasicStringLiteralContext<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<String> {
        let text = self.STRING().unwrap().get_text();
        Ok(text[1..text.len() - 1].to_string())
    }
}
impl Binder<String> for UnicodeStringLiteralContext<'_> {}
#[route(
    rowType,
    intervalType,
    dateTimeType,
    doublePrecisionType,
    legacyArrayType,
    legacyMapType,
    arrayType,
    genericType
)]
impl Binder<DataType> for Type_ContextAll<'_> {}
impl Binder<DataType> for RowTypeContext<'_> {}
impl Binder<DataType> for IntervalTypeContext<'_> {}
impl Binder<DataType> for DateTimeTypeContext<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<DataType> {
        if self.precision.is_some() {
            todo!()
        }
        if self.WITH().is_some() || self.WITHOUT().is_some() {
            todo!()
        }
        match self.base_.unwrap().get_token_type() {
            TIMESTAMP => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            _ => todo!(),
        }
    }
}

impl Binder<DataType> for DoublePrecisionTypeContext<'_> {}
impl Binder<DataType> for LegacyArrayTypeContext<'_> {}
impl Binder<DataType> for LegacyMapTypeContext<'_> {}
impl Binder<DataType> for ArrayTypeContext<'_> {}
impl Binder<DataType> for GenericTypeContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<DataType> {
        let type_ = self.identifier().unwrap().bind(bc)?.to_lowercase();
        match type_.as_str() {
            "real" => Ok(DataType::Float32),
            "decimal" => match self.typeParameter_all().len() {
                0 => make_decimal_type(None, None),
                1 => make_decimal_type(
                    Some(
                        self.typeParameter(0)
                            .unwrap()
                            .get_text()
                            .parse::<u64>()
                            .unwrap(),
                    ),
                    None,
                ),
                2 => make_decimal_type(
                    Some(
                        self.typeParameter(0)
                            .unwrap()
                            .get_text()
                            .parse::<u64>()
                            .unwrap(),
                    ),
                    Some(
                        self.typeParameter(1)
                            .unwrap()
                            .get_text()
                            .parse::<u64>()
                            .unwrap(),
                    ),
                ),
                _ => todo!(),
            },
            "tinyint" => Ok(DataType::Int8),
            "float" => Ok(DataType::Float32),
            "date" => Ok(DataType::Date32),
            "integer" => Ok(DataType::Int32),
            "int" => Ok(DataType::Int32),

            _ => todo!(),
        }
    }
}
impl Binder<WhenClause> for WhenClauseContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<WhenClause> {
        let condition = self.condition.as_ref().unwrap().bind(bc)?;
        let result = self.result.as_ref().unwrap().bind(bc)?;
        Ok(WhenClause { condition, result })
    }
}
impl Binder<Expr> for FilterContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        self.booleanExpression().unwrap().bind(bc)
    }
}
impl Binder<WindowSpecification> for OverContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<WindowSpecification> {
        if self.windowName.as_ref().is_some() {
            todo!();
        } else {
            self.windowSpecification().unwrap().bind(bc)
        }
    }
}
impl Binder<WindowFrame> for WindowFrameContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<WindowFrame> {
        if self.MEASURES().is_some() {
            todo!();
        }
        if self.AFTER().is_some() {
            todo!();
        }
        if self.INITIAL().is_some() {
            todo!();
        }
        if self.SEEK().is_some() {
            todo!();
        }
        if self.PATTERN().is_some() {
            todo!();
        }
        if self.SUBSET().is_some() {
            todo!();
        }
        if self.DEFINE().is_some() {
            todo!();
        }
        self.frameExtent().unwrap().bind(bc)
    }
}
impl Binder<WindowFrame> for FrameExtentContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<WindowFrame> {
        let units = match self.frameType.as_ref().unwrap().get_token_type() {
            RANGE => Ok(WindowFrameUnits::Range),
            ROWS => Ok(WindowFrameUnits::Rows),
            GROUPS => Ok(WindowFrameUnits::Groups),
            _ => Err(DataFusionError::Parser(Parser2Error {
                row: self.start().line,
                col: self.start().column,
                message: format!("unknown window frame unit {}", self.get_text()),
            })),
        }?;
        let start_bound = self.start.as_ref().unwrap().bind(bc)?;
        let end_bound = if let Some(e) = self.end.as_ref() {
            e.bind(bc)?
        } else {
            WindowFrameBound::CurrentRow
        };
        Ok(WindowFrame {
            units,
            start_bound,
            end_bound,
        })
    }
}
#[route(unboundedFrame, currentRowBound, boundedFrame)]
impl Binder<WindowFrameBound> for FrameBoundContextAll<'_> {}
impl Binder<WindowFrameBound> for UnboundedFrameContext<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<WindowFrameBound> {
        match self.boundType.as_ref().unwrap().get_token_type() {
            PRECEDING => Ok(WindowFrameBound::Preceding(ScalarValue::Null)),
            FOLLOWING => Ok(WindowFrameBound::Following(ScalarValue::Null)),
            _ => todo!(),
        }
    }
}
impl Binder<WindowFrameBound> for CurrentRowBoundContext<'_> {}
impl Binder<WindowFrameBound> for BoundedFrameContext<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<WindowFrameBound> {
        let value = if let Some(value) = maybe_get_int(self.expression().unwrap()) {
            ScalarValue::Utf8(Some(format!("{}", value)))
        } else if let Some(value) = maybe_get_string(self.expression().unwrap()) {
            ScalarValue::Utf8(Some(value))
        } else {
            // Interval
            todo!()
        };

        match self.boundType.as_ref().unwrap().get_token_type() {
            PRECEDING => Ok(WindowFrameBound::Preceding(value)),
            FOLLOWING => Ok(WindowFrameBound::Following(value)),
            _ => todo!(),
        }
    }
}
impl Binder<OwnedTableReference> for QualifiedNameContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<OwnedTableReference> {
        let identifiers: Vec<_> = self.bind(bc)?;
        if identifiers.len() == 1 {
            Ok(OwnedTableReference::Bare {
                table: identifiers[0].clone(),
            })
        } else if identifiers.len() == 2 {
            Ok(OwnedTableReference::Partial {
                schema: identifiers[0].clone(),
                table: identifiers[1].clone(),
            })
        } else if identifiers.len() == 3 {
            Ok(OwnedTableReference::Full {
                catalog: identifiers[0].clone(),
                schema: identifiers[1].clone(),
                table: identifiers[2].clone(),
            })
        } else {
            Err(DataFusionError::Bind(BinderError {
                row: self.start().line,
                col: self.start().column,
                message: "Cannot bind TableReference".to_owned(),
            }))
        }
    }
}

impl Binder<Vec<String>> for QualifiedNameContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Vec<String>> {
        self.identifier_all().iter().map(|i| i.bind(bc)).collect()
    }
}
impl Binder<Expr> for QualifiedNameContextAll<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let identifiers: Vec<_> = self.bind(bc)?;
        if identifiers.len() == 1 {
            let name = &identifiers[0];
            let location = CodeLocation::new(self.start().line, self.start().column);
            bc.resolve_column(&location, name.as_str())
        } else {
            todo!()
        }
    }
}
#[route(
    unquotedIdentifier,
    quotedIdentifier,
    backQuotedIdentifier,
    digitIdentifier
)]
impl Binder<String> for IdentifierContextAll<'_> {}
impl Binder<String> for UnquotedIdentifierContext<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<String> {
        Ok(self.get_text())
    }
}
impl Binder<String> for QuotedIdentifierContext<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<String> {
        let text = self.get_text();
        let len = text.len();
        Ok(text[1..len - 1].to_string())
    }
}
impl Binder<String> for BackQuotedIdentifierContext<'_> {}
impl Binder<String> for DigitIdentifierContext<'_> {}
#[route(decimalLiteral, doubleLiteral, integerLiteral)]
impl Binder<Expr> for NumberContextAll<'_> {}
impl Binder<Expr> for DecimalLiteralContext<'_> {
    fn bind(&self, bc: &BindingContextStack) -> Result<Expr> {
        let text = self.get_text();
        let n = text.as_str();
        if bc.float_as_decimal() {
            let str = n.trim_start_matches('0');
            if let Some(i) = str.find('.') {
                let p = str.len() - 1;
                let s = str.len() - i - 1;
                let str = str.replace('.', "");
                let n = str.parse::<i128>().map_err(|_| {
                    DataFusionError::Bind(BinderError {
                        row: self.start().line,
                        col: self.start().column,
                        message: format!(
                            "Cannot parse {str} as i128 when building decimal"
                        ),
                    })
                })?;
                Ok(Expr::Literal(ScalarValue::Decimal128(
                    Some(n),
                    p as u8,
                    s as i8,
                )))
            } else {
                let number = n.parse::<i128>().map_err(|_| {
                    DataFusionError::Bind(BinderError {
                        row: self.start().line,
                        col: self.start().column,
                        message: format!(
                            "Cannot parse {n} as i128 when building decimal"
                        ),
                    })
                })?;
                Ok(Expr::Literal(ScalarValue::Decimal128(Some(number), 38, 0)))
            }
        } else {
            match n.parse::<f64>().map(lit).map_err(|_| {
                DataFusionError::Bind(BinderError {
                    row: self.start().line,
                    col: self.start().column,
                    message: format!("Cannot parse {n} as f64"),
                })
            }) {
                Ok(e) => Ok(e),
                Err(e) => Err(e),
            }
        }
    }
}
impl Binder<Expr> for DoubleLiteralContext<'_> {}
impl Binder<Expr> for IntegerLiteralContext<'_> {
    fn bind(&self, _: &BindingContextStack) -> Result<Expr> {
        Ok(Expr::Literal(ScalarValue::Int64(Some(
            self.get_text().as_str().parse::<i64>().unwrap(),
        ))))
    }
}

// End generated boiler plate
