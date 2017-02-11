#![feature(box_patterns)]

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

#[derive(Debug)]
pub enum Expr {
    /// index into a value within the tuple
    TupleValue(usize),
    /// literal value
    Literal(Value),
    /// binary expression e.g. "age > 21"
    BinaryExpr { left: Box<Expr>, op: Operator, right: Box<Expr> },
}

/// Query plan
#[derive(Debug)]
pub enum PlanNode {
    TableScan { schema: String, table: String },
    Filter { expr: Expr, input: Box<PlanNode> },
    Project { expr: Vec<Expr>, input: Box<PlanNode> }
}
