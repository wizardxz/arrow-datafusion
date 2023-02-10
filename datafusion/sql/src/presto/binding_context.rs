use arrow_schema::DataType;
use datafusion_expr::{Expr, ExprSchemable, Join, JoinConstraint, LogicalPlan};

use super::function::FunctionOverload;
use datafusion_common::{BinderError, Column, DataFusionError, Result, TableReference};
use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
};

pub trait BindingContext {
    fn resolve_table(&self, _: &TableReference) -> Result<LogicalPlan> {
        Err(DataFusionError::BindingContextInternal(None))
    }

    fn resolve_column(&self, _: &str) -> Result<Expr> {
        Err(DataFusionError::BindingContextInternal(None))
    }

    fn resolve_qualified_column(&self, _: &str, _: &str) -> Result<Expr> {
        Err(DataFusionError::BindingContextInternal(None))
    }

    fn get_expr_type(&self, _: &Expr) -> Result<DataType> {
        Err(DataFusionError::BindingContextInternal(None))
    }

    fn float_as_decimal(&self) -> Result<bool> {
        Err(DataFusionError::BindingContextInternal(None))
    }

    fn resolve_function(
        &self,
        _: &str,
        _: &Vec<DataType>,
    ) -> Result<Rc<FunctionOverload>> {
        Err(DataFusionError::BindingContextInternal(None))
    }

    fn get_value(&self) -> Result<Expr> {
        Err(DataFusionError::BindingContextInternal(None))
    }

    fn get_groupby_ordinal(&self, _: usize) -> Result<Expr> {
        Err(DataFusionError::BindingContextInternal(None))
    }

    fn get_groupby_alias(&self, _: &str) -> Result<Expr> {
        Err(DataFusionError::BindingContextInternal(None))
    }
}

pub struct ColumnBindingContext<'a> {
    pub parent: &'a LogicalPlan,
}

fn get_relation_name(parent: &LogicalPlan) -> Option<String> {
    match parent {
        LogicalPlan::TableScan(t) => Some(t.table_name.clone()),
        LogicalPlan::SubqueryAlias(s) => Some(s.alias.clone()),
        LogicalPlan::Filter(f) => get_relation_name(f.input.as_ref()),
        _ => None,
    }
}

impl<'a> BindingContext for ColumnBindingContext<'a> {
    fn resolve_column(&self, name: &str) -> Result<Expr> {
        match self.parent {
            LogicalPlan::Join(join) => {
                let left_result =
                    ColumnBindingContext { parent: &join.left }.resolve_column(name);
                let right_result = ColumnBindingContext {
                    parent: &join.right,
                }
                .resolve_column(name);
                match (left_result, right_result) {
                    (Err(_), Err(_)) => Err(DataFusionError::BindingContextInternal(
                        Some(format!("No column {name} found")),
                    )),
                    (Ok(left), Err(_)) => Ok(left),
                    (Err(_), Ok(right)) => Ok(right),
                    (Ok(left), Ok(_)) => {
                        if let LogicalPlan::Join(Join {
                            join_constraint: JoinConstraint::Using,
                            ..
                        }) = self.parent
                        {
                            Ok(left)
                        } else {
                            Err(DataFusionError::BindingContextInternal(Some(format!(
                                "Ambiguous column {name} found"
                            ))))
                        }
                    }
                }
            }
            LogicalPlan::CrossJoin(join) => {
                let left_result =
                    ColumnBindingContext { parent: &join.left }.resolve_column(name);
                let right_result = ColumnBindingContext {
                    parent: &join.right,
                }
                .resolve_column(name);
                match (left_result, right_result) {
                    (Err(_), Err(_)) => Err(DataFusionError::BindingContextInternal(
                        Some(format!("No column {name} found")),
                    )),
                    (Ok(left), Err(_)) => Ok(left),
                    (Err(_), Ok(right)) => Ok(right),
                    (Ok(_), Ok(_)) => Err(DataFusionError::BindingContextInternal(Some(
                        format!("Ambiguous column {name} found"),
                    ))),
                }
            }
            _ => {
                let relation = get_relation_name(&self.parent);

                match self
                    .parent
                    .schema()
                    .fields_with_unqualified_name(name)
                    .len()
                {
                    0 => Err(DataFusionError::BindingContextInternal(Some(format!(
                        "No column {name} found"
                    )))),
                    1 => Ok(Expr::Column(Column {
                        relation: relation,
                        name: name.to_string(),
                    })),
                    _ => Err(DataFusionError::BindingContextInternal(Some(format!(
                        "Ambiguous column {name} found"
                    )))),
                }
            }
        }
    }

    fn resolve_qualified_column(&self, qualifier: &str, name: &str) -> Result<Expr> {
        if let Some(parent_name) = get_relation_name(self.parent) {
            if parent_name.split(".").last().unwrap_or("") == qualifier {
                return self.resolve_column(name);
            }
        }
        match self.parent {
            LogicalPlan::Join(join) => {
                let bc = ColumnBindingContext { parent: &join.left };
                match bc.resolve_qualified_column(qualifier, name) {
                    Ok(expr) => Ok(expr),
                    Err(_) => {
                        let bc = ColumnBindingContext {
                            parent: &join.right,
                        };
                        match bc.resolve_qualified_column(qualifier, name) {
                            Ok(expr) => Ok(expr),
                            Err(e) => Err(e),
                        }
                    }
                }
            }
            LogicalPlan::CrossJoin(join) => {
                let bc = ColumnBindingContext { parent: &join.left };
                match bc.resolve_qualified_column(qualifier, name) {
                    Ok(expr) => Ok(expr),
                    Err(_) => {
                        let bc = ColumnBindingContext {
                            parent: &join.right,
                        };
                        match bc.resolve_qualified_column(qualifier, name) {
                            Ok(expr) => Ok(expr),
                            Err(e) => Err(e),
                        }
                    }
                }
            }
            LogicalPlan::Filter(f) => {
                let bc = ColumnBindingContext { parent: &f.input };
                bc.resolve_qualified_column(qualifier, name)
            }
            _ => Err(DataFusionError::BindingContextInternal(Some(format!(
                "No column {}.{} found",
                qualifier, name
            )))),
        }
    }

    fn get_expr_type(&self, expr: &Expr) -> Result<DataType> {
        expr.get_type(self.parent.schema())
    }
}

pub struct MultipleParentColumnBindingContext<'a> {
    pub parents: Vec<&'a LogicalPlan>,
}

impl<'a> BindingContext for MultipleParentColumnBindingContext<'a> {
    fn resolve_column(&self, name: &str) -> Result<Expr> {
        let results = self
            .parents
            .iter()
            .filter_map(|parent| {
                ColumnBindingContext { parent }.resolve_column(name).ok()
            })
            .collect::<Vec<_>>();
        match results.len() {
            0 => Err(DataFusionError::BindingContextInternal(Some(format!(
                "No column {name} found"
            )))),
            1 => Ok(results.into_iter().next().unwrap()),
            _ => Err(DataFusionError::BindingContextInternal(Some(format!(
                "Ambiguous column {name} found"
            )))),
        }
    }

    fn resolve_qualified_column(&self, qualifier: &str, name: &str) -> Result<Expr> {
        if let Some((first, rest)) = self.parents.split_first() {
            let result = ColumnBindingContext { parent: first }
                .resolve_qualified_column(qualifier, name);
            if result.is_ok() {
                return result;
            } else if !rest.is_empty() {
                MultipleParentColumnBindingContext {
                    parents: rest.to_vec(),
                }
                .resolve_qualified_column(qualifier, name)
            } else {
                Err(DataFusionError::BindingContextInternal(Some(format!(
                    "No column {name} found"
                ))))
            }
        } else {
            Err(DataFusionError::Internal(format!(
                "MultipleParentColumnBindingContext should have logical plans."
            )))
        }
    }

    fn get_expr_type(&self, expr: &Expr) -> Result<DataType> {
        let results = self
            .parents
            .iter()
            .filter_map(|parent| ColumnBindingContext { parent }.get_expr_type(expr).ok())
            .collect::<HashSet<_>>();
        match results.len() {
            0 => Err(DataFusionError::BindingContextInternal(Some(format!(
                "No expr {expr:?} found"
            )))),
            1 => Ok(results.into_iter().next().unwrap()),
            _ => Err(DataFusionError::BindingContextInternal(Some(format!(
                "Ambiguous expr type {expr:?} found"
            )))),
        }
    }
}

pub struct FloatAsDecimalContext {
    pub enabled: bool,
}

impl FloatAsDecimalContext {
    pub fn new(enabled: bool) -> Self {
        FloatAsDecimalContext { enabled: enabled }
    }
}

impl BindingContext for FloatAsDecimalContext {
    fn float_as_decimal(&self) -> Result<bool> {
        Ok(self.enabled)
    }
}

pub struct FunctionBindingContext {
    overloads: HashMap<String, Vec<Rc<FunctionOverload>>>,
}

impl FunctionBindingContext {
    pub fn new(
        overloads: HashMap<String, Vec<Rc<FunctionOverload>>>,
    ) -> FunctionBindingContext {
        FunctionBindingContext { overloads }
    }
}

impl BindingContext for FunctionBindingContext {
    fn resolve_function(
        &self,
        name: &str,
        argument_types: &Vec<DataType>,
    ) -> Result<Rc<FunctionOverload>> {
        match self.overloads.get(name.to_lowercase().as_str()) {
            Some(overloads) => {
                for overload in overloads {
                    if overload.as_ref().is_match(name, argument_types) {
                        return Ok(overload.clone());
                    }
                }
                let candidate_signatures = overloads
                    .iter()
                    .map(|o| format!("{:?}", o.argument_types))
                    .collect::<Vec<_>>();
                Err(DataFusionError::BindingContextInternal(Some(format!(
                    "Function {} arguments do not match, actual {:?}, candidates {:?}",
                    name, argument_types, candidate_signatures
                ))))
            }
            None => Err(DataFusionError::BindingContextInternal(Some(format!(
                "No function overload found {name}"
            )))),
        }
    }
}

pub struct ValueContext<'a> {
    pub value: &'a Expr,
}

impl<'a> BindingContext for ValueContext<'a> {
    fn get_value(&self) -> Result<Expr> {
        Ok(self.value.clone())
    }
}

pub struct GroupbyExpressionContext<'a> {
    pub items: &'a Vec<Expr>,
}

impl<'a> BindingContext for GroupbyExpressionContext<'a> {
    fn get_groupby_ordinal(&self, ordinal: usize) -> Result<Expr> {
        if !(ordinal >= 1 && ordinal <= self.items.len()) {
            return Err(DataFusionError::BindingContextInternal(Some(format!(
                "Projection references non-aggregate values: ordinal {} could not be resolved",
                ordinal
            ))));
        }
        match self.items.get(ordinal - 1) {
            Some(expr) => match expr {
                Expr::Alias(expr, _) => Ok(*expr.clone()),
                _ => Ok(expr.clone()),
            },
            None => Err(DataFusionError::BindingContextInternal(Some(format!(
                "Projection references non-aggregate values: ordinal {} could not be resolved",
                ordinal
            )))),
        }
    }

    fn get_groupby_alias(&self, alias: &str) -> Result<Expr> {
        match self.items.iter().find(|&x| match x {
            Expr::Alias(_, a) => a == alias,
            _ => false,
        }) {
            Some(Expr::Alias(expr, _)) => Ok(*expr.clone()),
            _ => Err(DataFusionError::BindingContextInternal(Some(format!(
                "cannot get alias {}",
                alias
            )))),
        }
    }
}

#[derive(Clone)]
pub struct BindingContextStack<'a> {
    stack: Vec<&'a dyn BindingContext>,
}

impl<'a> BindingContextStack<'a> {
    pub fn new(stack: Vec<&'a dyn BindingContext>) -> Self {
        BindingContextStack { stack: stack }
    }

    pub fn push(&self, bc: &'a dyn BindingContext) -> BindingContextStack<'a> {
        let mut new_stack = self.stack.clone();
        new_stack.push(bc);
        BindingContextStack::new(new_stack)
    }

    pub fn with<F, T>(context: &BindingContextStack, f: F) -> T
    where
        F: Fn(&BindingContextStack) -> T,
    {
        f(&context)
    }

    pub fn with_push<F, T>(&self, context: &dyn BindingContext, f: F) -> T
    where
        F: Fn(&BindingContextStack) -> T,
    {
        let new_context = self.push(context);
        BindingContextStack::with(&new_context, f)
    }

    fn resolve<F, T>(&self, f: F) -> Result<T>
    where
        F: Fn(&dyn BindingContext) -> Result<T>,
    {
        let mut best_error_message: Option<String> = None;
        for bc in self.stack.iter().rev() {
            let result = f(*bc);
            if result.is_ok() {
                return result;
            } else {
                if let Err(DataFusionError::BindingContextInternal(e)) = result {
                    best_error_message = best_error_message.or(e);
                }
            }
        }
        Err(DataFusionError::BindingContextInternal(
            best_error_message.clone(),
        ))
    }

    pub fn resolve_table(
        &self,
        location: &CodeLocation,
        table_ref: &TableReference,
    ) -> Result<LogicalPlan> {
        match self.resolve(|bc| bc.resolve_table(table_ref)) {
            Ok(result) => Ok(result),
            Err(DataFusionError::BindingContextInternal(Some(e))) => {
                Err(DataFusionError::Bind(BinderError {
                    row: location.row,
                    col: location.col,
                    message: e,
                }))
            }
            Err(e) => Err(DataFusionError::Internal(format!(
                "({},{}) unexpected error in resolve_table {:?}",
                location.row, location.col, e
            ))),
        }
    }

    pub fn resolve_column(&self, location: &CodeLocation, name: &str) -> Result<Expr> {
        match self.resolve(|bc| bc.resolve_column(name)) {
            Ok(result) => Ok(result),
            Err(DataFusionError::BindingContextInternal(Some(e))) => {
                Err(DataFusionError::Bind(BinderError {
                    row: location.row,
                    col: location.col,
                    message: e,
                }))
            }
            _ => Err(DataFusionError::Internal(format!(
                "({},{}) unexpected error in resolve_column",
                location.row, location.col
            ))),
        }
    }

    pub fn resolve_qualified_column(
        &self,
        location: &CodeLocation,
        qualifier: &str,
        name: &str,
    ) -> Result<Expr> {
        match self.resolve(|bc| bc.resolve_qualified_column(qualifier, name)) {
            Ok(result) => Ok(result),
            Err(DataFusionError::BindingContextInternal(Some(e))) => {
                Err(DataFusionError::Bind(BinderError {
                    row: location.row,
                    col: location.col,
                    message: e,
                }))
            }
            _ => Err(DataFusionError::Internal(format!(
                "({},{}) unexpected error in resolve_qualified_column",
                location.row, location.col
            ))),
        }
    }

    pub fn get_expr_type(
        &self,
        location: &CodeLocation,
        expr: &Expr,
    ) -> Result<DataType> {
        match self.resolve(|bc| bc.get_expr_type(expr)) {
            Ok(result) => Ok(result),
            _ => Err(DataFusionError::Internal(format!(
                "({},{}) unexpected error in get_expr_type",
                location.row, location.col
            ))),
        }
    }

    pub fn float_as_decimal(&self) -> bool {
        match self.resolve(|bc| bc.float_as_decimal()) {
            Ok(result) => result,
            Err(_) => false,
        }
    }

    pub fn resolve_function(
        &self,
        location: &CodeLocation,
        name: &str,
        argument_types: &Vec<DataType>,
    ) -> Result<Rc<FunctionOverload>> {
        match self.resolve(|bc| bc.resolve_function(name, argument_types)) {
            Ok(result) => Ok(result),
            Err(DataFusionError::BindingContextInternal(Some(e))) => {
                Err(DataFusionError::Bind(BinderError {
                    row: location.row,
                    col: location.col,
                    message: e,
                }))
            }
            _ => Err(DataFusionError::Internal(format!(
                "({},{}) unexpected error in resolve_function",
                location.row, location.col
            ))),
        }
    }

    pub fn get_value(&self, location: &CodeLocation) -> Result<Expr> {
        match self.resolve(|bc| bc.get_value()) {
            Ok(result) => Ok(result),
            _ => Err(DataFusionError::Internal(format!(
                "({},{}) unexpected error in get_value",
                location.row, location.col
            ))),
        }
    }

    pub fn get_groupby_ordinal(
        &self,
        location: &CodeLocation,
        ordinal: usize,
    ) -> Result<Expr> {
        match self.resolve(|bc| bc.get_groupby_ordinal(ordinal)) {
            Ok(result) => Ok(result),
            Err(DataFusionError::BindingContextInternal(Some(e))) => {
                Err(DataFusionError::Bind(BinderError {
                    row: location.row,
                    col: location.col,
                    message: e,
                }))
            }
            _ => Err(DataFusionError::Internal(format!(
                "({},{}) unexpected error in get_groupby_ordinal",
                location.row, location.col
            ))),
        }
    }

    pub fn get_groupby_alias(
        &self,
        location: &CodeLocation,
        alias: &str,
    ) -> Result<Expr> {
        match self.resolve(|bc| bc.get_groupby_alias(alias)) {
            Ok(result) => Ok(result),
            Err(DataFusionError::BindingContextInternal(Some(e))) => {
                Err(DataFusionError::Bind(BinderError {
                    row: location.row,
                    col: location.col,
                    message: e,
                }))
            }
            _ => Err(DataFusionError::Internal(format!(
                "({},{}) unexpected error in get_groupby_alias",
                location.row, location.col
            ))),
        }
    }
}

#[derive(Debug)]
pub struct CodeLocation {
    pub row: isize,
    pub col: isize,
}

impl CodeLocation {
    pub fn new(row: isize, col: isize) -> Self {
        CodeLocation { row: row, col: col }
    }
}

impl std::fmt::Display for CodeLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}, {})", self.row, self.col)
    }
}
