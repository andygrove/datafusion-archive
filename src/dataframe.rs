use super::rel::*;

#[derive(Debug)]
pub enum DataFrameError {
    NotImplemented
}

pub struct Column {
}

impl Column {

    pub fn eq(&self, v: Value) -> Rex {
        //TODO: return Rex::BinaryExpr
        unimplemented!()
    }
}

/// DataFrame is an abstraction of a distributed query plan
pub trait DataFrame {

    /// Change the number of partitions
    fn repartition(&self, n: u32) -> Result<Box<DataFrame>,DataFrameError>;

    /// Projection
    fn select(&self, expr: Vec<Rex>) -> Result<Box<DataFrame>,DataFrameError>;

    /// Selection
    fn filter(&self, expr: Rex) -> Result<Box<DataFrame>,DataFrameError>;

    /// Write to CSV ...  will support other formats in the future
    fn write(&self, filename: &str) -> Result<Box<DataFrame>,DataFrameError>;

    /// Return an expression representing the specified column
    fn col(&self, column_name: &str) -> Result<Column,DataFrameError>;
}

