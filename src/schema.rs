#![feature(box_patterns)]

use std::error::Error;
use std::fs::File;

/// The data types supported by this database. Currently just u64 and string but others
/// will be added later, including complex types
#[derive(Debug,Clone)]
pub enum DataType {
    UnsignedLong,
    String
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

/// Value holder for all supported data types
#[derive(Debug,Clone,PartialEq,PartialOrd)]
pub enum Value {
    UnsignedLong(u64),
    String(String),
    Boolean(bool)
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


