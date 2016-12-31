#![feature(box_patterns)]

use std::error::Error;

/// The data types supported by this database. Currently just u64 and string but others
/// will be added later, including complex types
#[derive(Debug,Clone)]
enum DataType {
    UnsignedLong,
    String
}

/// Definition of a column in a relation (data set).
#[derive(Debug,Clone)]
struct ColumnMeta {
    name: String,
    data_type: DataType,
    nullable: bool
}

/// Definition of a relation (data set) consisting of one or more columns.
#[derive(Debug,Clone)]
struct TupleType {
    columns: Vec<ColumnMeta>
}

/// Value holder for all supported data types
#[derive(Debug,Clone)]
enum Value {
    UnsignedLong(u64),
    String(String),
    Boolean(bool)
}

/// A tuple represents one row within a relation and is implemented as a trait to allow for
/// specific implementations for different data sources
trait Tuple {
    fn get_value(&self, index: usize) -> Result<Value, Box<Error>>;
}

/// A simple tuple implementation for testing and initial prototyping
#[derive(Debug)]
struct SimpleTuple {
    values: Vec<Value>
}

impl Tuple for SimpleTuple {

    fn get_value(&self, index: usize) -> Result<Value, Box<Error>> {
        Ok(self.values[index].clone())
    }

}

#[derive(Debug)]
enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug)]
enum Expr {
    /// index into a value within the tuple
    TupleValue(usize),
    /// literal value
    Literal(Value),
    /// binary expression e.g. "age > 21"
    BinaryExpr { left: Box<Expr>, op: Operator, right: Box<Expr> },
}

#[derive(Debug)]
enum PlanNode {
//    TableScan,
//    IndexScan,
    Filter(Expr),
//    Sort,
//    Project,
//    Join
}

fn evaluate(tuple: &Box<Tuple>, tt: &TupleType, expr: &Expr) -> Result<Value, Box<Error>> {

    match expr {
        &Expr::BinaryExpr { box ref left, ref op, box ref right } => {
            let left_value = evaluate(tuple, tt, left).unwrap();
            let right_value = evaluate(tuple, tt, right).unwrap();
            match op {
                &Operator::Eq => {
                    match left_value {
                        Value::UnsignedLong(l) => match right_value {
                            Value::UnsignedLong(r) => Ok(Value::Boolean(l == r)),
                            _ => Err(From::from("oops"))
                        },
                        _ => Err(From::from("oops"))
                    }
                },
                _ => Err(From::from("oops"))
            }
        },
        &Expr::TupleValue(index) => tuple.get_value(index),
        &Expr::Literal(ref value) => Ok(value.clone()),
    }

}

fn main() {

    let tt = TupleType {
        columns: vec![
            ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    println!("Tuple type: {:?}", tt);

    let data : Vec<Box<Tuple>> = vec![
        Box::new(SimpleTuple { values: vec![ Value::UnsignedLong(1), Value::String(String::from("Alice")) ] }),
        Box::new(SimpleTuple { values: vec![ Value::UnsignedLong(2), Value::String(String::from("Bob")) ] }),
    ];

    // create simple filter expression for "id = 2"
    let filterExpr = Expr::BinaryExpr {
        left: Box::new(Expr::TupleValue(0)),
        op: Operator::Eq,
        right: Box::new(Expr::Literal(Value::UnsignedLong(2)))
    };

    println!("Expression: {:?}", filterExpr);

    // iterate over tuples and evaluate the expression
    for tuple in &data {
        let x = evaluate(tuple, &tt, &filterExpr).unwrap();

        println!("filter expr evaluates to {:?}", x);
    }

}
