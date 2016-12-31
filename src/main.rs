#![feature(box_patterns)]

use std::error::Error;

#[derive(Debug)]
enum DataType {
    UnsignedLong,
    String
}

#[derive(Debug)]
struct ColumnType {
    name: String,
    data_type: DataType,
    nullable: bool
}

#[derive(Debug)]
struct TupleType {
    columns: Vec<ColumnType>
}

#[derive(Debug,Clone)]
enum Value {
    UnsignedLong(u64),
    String(String),
    Boolean(bool)
}

/// The tuple trait provides type-safe access to individual values within the tuple
trait Tuple {
    fn get_value(&self, index: usize) -> Result<Value, Box<Error>>;
}

#[derive(Debug)]
struct SimpleTuple {
    values: Vec<Value>
}

impl Tuple for SimpleTuple {

    fn get_value(&self, index: usize) -> Result<Value, Box<Error>> {
        Ok(self.values[index].clone())
    }

}

enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

enum Expr {
    /// index into a value within the tuple
    TupleValue(usize),
    /// literal value
    Literal(Value),
    /// binary expression e.g. "age > 21"
    BinaryExpr { left: Box<Expr>, op: Operator, right: Box<Expr> },
}

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
            ColumnType { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnType { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    println!("Tuple type: {:?}", tt);

    let data : Vec<Box<Tuple>> = vec![
        Box::new(SimpleTuple { values: vec![ Value::UnsignedLong(1), Value::String(String::from("Alice")) ] }),
        Box::new(SimpleTuple { values: vec![ Value::UnsignedLong(2), Value::String(String::from("Bob")) ] }),
    ];

    // create simple filter expression
    let filterExpr = Expr::BinaryExpr {
        left: Box::new(Expr::TupleValue(0)),
        op: Operator::Eq,
        right: Box::new(Expr::Literal(Value::UnsignedLong(2)))
    };

    //let plan = PlanNode::Filter(filterExpr);

    // iterate over tuples and evaluate the plan
    for tuple in &data {
        let x = evaluate(tuple, &tt, &filterExpr).unwrap();

        print!("filter expr evaluates to {:?}", x);
    }

}
