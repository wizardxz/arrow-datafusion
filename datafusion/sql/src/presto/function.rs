use std::{collections::HashMap, rc::Rc};

use arrow::compute::can_cast_types;
use arrow_schema::DataType;
pub enum FunctionType {
    Scalar,
    Aggregate,
    Window,
}

#[derive(Debug)]
struct MatchingResult {
    result: bool,
    constraint: Rc<HashMap<GenericPrimitiveType, DataType>>,
}

impl MatchingResult {
    fn new(
        result: bool,
        constraint: Rc<HashMap<GenericPrimitiveType, DataType>>,
    ) -> Self {
        MatchingResult {
            result: result,
            constraint: constraint,
        }
    }
}

trait Match {
    fn match_(
        &self,
        type_: &DataType,
        constraint: Rc<HashMap<GenericPrimitiveType, DataType>>,
    ) -> MatchingResult;
}

#[derive(Debug, Clone)]
pub enum GenericDataType {
    GenericType(GenericType),
    DataType(DataType),
}

impl GenericDataType {
    pub fn new(data_type: DataType) -> Self {
        GenericDataType::DataType(data_type)
    }

    pub fn new_generic_primitive(name: &str) -> Self {
        GenericDataType::GenericType(GenericType::Primitive(GenericPrimitiveType {
            name: name.to_string(),
            bound: vec![],
        }))
    }

    pub fn new_generic_primitive_bounded(name: &str, bound: Vec<DataType>) -> Self {
        GenericDataType::GenericType(GenericType::Primitive(GenericPrimitiveType {
            name: name.to_string(),
            bound: bound,
        }))
    }

    pub fn new_generic_array(item: Box<GenericDataType>) -> Self {
        GenericDataType::GenericType(GenericType::Compound(GenericCompoundType::Array(
            item,
        )))
    }

    pub fn new_generic_map(
        key: Box<GenericDataType>,
        value: Box<GenericDataType>,
    ) -> Self {
        GenericDataType::GenericType(GenericType::Compound(GenericCompoundType::Map((
            key, value,
        ))))
    }

    pub fn new_generic_struct(fields: Vec<(String, Box<GenericDataType>)>) -> Self {
        GenericDataType::GenericType(GenericType::Compound(GenericCompoundType::Struct(
            fields,
        )))
    }
}

impl Match for GenericDataType {
    fn match_(
        &self,
        type_: &DataType,
        constraint: Rc<HashMap<GenericPrimitiveType, DataType>>,
    ) -> MatchingResult {
        match self {
            GenericDataType::DataType(dt) => {
                MatchingResult::new(can_cast_types(type_, dt), constraint.clone())
            }
            GenericDataType::GenericType(gt) => gt.match_(type_, constraint),
        }
    }
}

#[derive(Debug, Clone)]
pub enum GenericType {
    Primitive(GenericPrimitiveType),
    Compound(GenericCompoundType),
}

impl Match for GenericType {
    fn match_(
        &self,
        type_: &DataType,
        constraint: Rc<HashMap<GenericPrimitiveType, DataType>>,
    ) -> MatchingResult {
        match self {
            GenericType::Primitive(g) => g.match_(type_, constraint),
            GenericType::Compound(g) => g.match_(type_, constraint),
        }
    }
}

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct GenericPrimitiveType {
    pub name: String,
    pub bound: Vec<DataType>,
}

impl Match for GenericPrimitiveType {
    fn match_(
        &self,
        type_: &DataType,
        constraint: Rc<HashMap<GenericPrimitiveType, DataType>>,
    ) -> MatchingResult {
        match constraint.get(self) {
            Some(dt) => {
                MatchingResult::new(can_cast_types(type_, dt), constraint.clone())
            }
            None => {
                if self.bound.len() == 0 || self.bound.contains(&type_) {
                    let new_constraint = constraint
                        .iter()
                        .chain(HashMap::from([(self, type_)]).into_iter())
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    MatchingResult::new(true, Rc::new(new_constraint))
                } else {
                    MatchingResult::new(false, constraint.clone())
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum GenericCompoundType {
    Array(Box<GenericDataType>),
    Map((Box<GenericDataType>, Box<GenericDataType>)),
    Struct(Vec<(String, Box<GenericDataType>)>),
}

impl Match for GenericCompoundType {
    fn match_(
        &self,
        _type_: &DataType,
        _constraint: Rc<HashMap<GenericPrimitiveType, DataType>>,
    ) -> MatchingResult {
        match self {
            GenericCompoundType::Array(_item) => todo!(),
            GenericCompoundType::Map((_key, _value)) => todo!(),
            GenericCompoundType::Struct(_fields) => todo!(),
        }
    }
}

pub struct FunctionOverload {
    pub name: String,
    pub type_: FunctionType,
    pub return_type: GenericDataType,
    pub argument_types: Vec<GenericDataType>,
    pub variant_arguments: bool,
}

impl FunctionOverload {
    pub fn is_match(&self, name: &str, argument_types: &Vec<DataType>) -> bool {
        if self.name != name.to_lowercase() {
            return false;
        }
        let expected_argument_types = if self.variant_arguments {
            self.argument_types
                .clone()
                .into_iter()
                .cycle()
                .take(argument_types.len())
                .collect::<Vec<_>>()
        } else {
            self.argument_types.clone()
        };

        if expected_argument_types.len() != argument_types.len() {
            return false;
        }
        let mut constraint = Rc::new(HashMap::new());
        expected_argument_types
            .iter()
            .zip(argument_types.iter())
            .all(|(expected, actual)| {
                let matching_result = expected.match_(actual, constraint.clone());
                constraint = matching_result.constraint;
                matching_result.result
            })
    }
}
