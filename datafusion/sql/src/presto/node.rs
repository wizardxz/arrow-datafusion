use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_expr::{Expr, WindowFrame};

pub struct GroupBy {
    pub quantifier: Quantifier,
    pub grouping_elements: Vec<GroupingElement>,
}

pub enum GroupingElement {
    Single(SingleGroupingElement),
    Rollup(RollupGroupingElement),
    Cube(CubeGroupingElement),
    Multiple(MultipleGroupingElement),
}

pub struct SingleGroupingElement {
    pub expr: Expr,
}
pub struct RollupGroupingElement {
    pub exprs: Vec<Expr>,
}
pub struct CubeGroupingElement {
    pub exprs: Vec<Expr>,
}
pub struct MultipleGroupingElement {
    pub grouping_sets: Vec<GroupingSet>,
}
pub struct GroupingSet {
    pub exprs: Vec<Expr>,
}

pub enum Quantifier {
    Distinct,
    All,
}

impl From<GroupingElement> for Expr {
    fn from(grouping_element: GroupingElement) -> Self {
        match grouping_element {
            GroupingElement::Single(g) => Expr::from(g),
            GroupingElement::Rollup(g) => Expr::from(g),
            GroupingElement::Cube(g) => Expr::from(g),
            GroupingElement::Multiple(g) => Expr::from(g),
        }
    }
}

impl From<SingleGroupingElement> for Expr {
    fn from(g: SingleGroupingElement) -> Self {
        g.expr
    }
}

impl From<RollupGroupingElement> for Expr {
    fn from(g: RollupGroupingElement) -> Self {
        Expr::GroupingSet(datafusion_expr::GroupingSet::Rollup(g.exprs))
    }
}

impl From<CubeGroupingElement> for Expr {
    fn from(g: CubeGroupingElement) -> Self {
        Expr::GroupingSet(datafusion_expr::GroupingSet::Cube(g.exprs))
    }
}

impl From<MultipleGroupingElement> for Expr {
    fn from(g: MultipleGroupingElement) -> Self {
        let exprs = g
            .grouping_sets
            .into_iter()
            .map(|item| item.exprs)
            .collect::<Vec<Vec<Expr>>>();

        Expr::GroupingSet(datafusion_expr::GroupingSet::GroupingSets(exprs))
    }
}

#[derive(Debug)]
pub struct WindowSpecification {
    pub partitions: Vec<Expr>,
    pub sort_items: Vec<Expr>,
    pub window_frame: WindowFrame,
}

pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

pub struct Properties {
    pub format: Option<String>,
    pub external_location: Option<String>,
    pub compression: CompressionTypeVariant,
}

pub struct Property {
    pub key: String,
    pub value: Expr,
}

impl Properties {
    pub fn new() -> Self {
        Properties {
            format: None,
            external_location: None,
            compression: CompressionTypeVariant::UNCOMPRESSED,
        }
    }
}
