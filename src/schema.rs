use super::rel::*;

/// The data types supported by this database. Currently just u64 and string but others
/// will be added later, including complex types
#[derive(Debug,Clone)]
pub enum DataType {
    UnsignedLong,
    String,
    Double
}

/// Definition of a column in a relation (data set).
#[derive(Debug,Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool
}

/// Definition of a relation (data set) consisting of one or more columns.
#[derive(Debug,Clone)]
pub struct TupleType {
    pub columns: Vec<ColumnMeta>
}


/// A tuple represents one row within a relation and is implemented as a trait to allow for
/// specific implementations for different data sources
//pub trait Tuple {
//    fn get_value(&self, index: usize) -> Result<Value, Box<Error>>;
//}

#[derive(Debug,Clone)]
pub struct Tuple {
    pub values: Vec<Value>
}

pub trait Relation<'a> {
    fn scan(&'a mut self) -> Box<Iterator<Item=Tuple> + 'a>;
}

pub trait DataFrame<T> {
    fn select(&mut self, column_names: Vec<String>) -> Box<Self>;


}


