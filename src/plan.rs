use std::error::Error;
use std::fs::File;

use super::schema::*;

#[derive(Debug)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// Relation Expression
#[derive(Debug)]
pub enum Rex {
    /// index into a value within the tuple
    TupleValue(usize),
    /// literal value
    Literal(Value),
    /// binary expression e.g. "age > 21"
    BinaryExpr { left: Box<Rex>, op: Operator, right: Box<Rex> },
}

/// Relations
#[derive(Debug)]
pub enum Rel {
    TableScan { schema: String, table: String },
    Filter { Rex: Rex, input: Box<Rel> },
    Project { Rex: Vec<Rex>, input: Box<Rel> }
}

