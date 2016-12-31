#![feature(box_patterns)]

use std::error::Error;

extern crate csv;
use csv::{Reader, StringRecords};

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
#[derive(Debug,Clone,PartialEq,PartialOrd)]
enum Value {
    UnsignedLong(u64),
    String(String),
    Boolean(bool)
}

/// A tuple represents one row within a relation and is implemented as a trait to allow for
/// specific implementations for different data sources
trait Tuple {
    fn get_value(&self, index: usize) -> Result<Value, Box<std::error::Error>>;
}

/// A simple tuple implementation for testing and initial prototyping
#[derive(Debug)]
struct SimpleTuple {
    values: Vec<Value>
}

impl Tuple for SimpleTuple {

    fn get_value(&self, index: usize) -> Result<Value, Box<std::error::Error>> {
        Ok(self.values[index].clone())
    }

}

/// dumb
trait DataSource {
    fn scan(&self) -> Result<Box<Iterator<Item=Tuple>>, Box<Error>>;
}

struct CsvDataSource {
}

impl DataSource for CsvDataSource {
    fn scan(&self) -> Result<Box<Iterator<Item=Tuple>>, Box<Error>> {
        Err(From::from("not implemented yet"))
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

/// Query plan
#[derive(Debug)]
enum PlanNode {
    TableScan,
    Filter(Expr),
    Project(Vec<Expr>),
}

fn evaluate(tuple: &Tuple, tt: &TupleType, expr: &Expr) -> Result<Value, Box<std::error::Error>> {

    match expr {
        &Expr::BinaryExpr { box ref left, ref op, box ref right } => {
            //TODO: remove use of unwrap() here
            let left_value = evaluate(tuple, tt, left).unwrap();
            let right_value = evaluate(tuple, tt, right).unwrap();
            match op {
                &Operator::Eq => Ok(Value::Boolean(left_value == right_value)),
                &Operator::NotEq => Ok(Value::Boolean(left_value != right_value)),
                &Operator::Lt => Ok(Value::Boolean(left_value < right_value)),
                &Operator::LtEq => Ok(Value::Boolean(left_value <= right_value)),
                &Operator::Gt => Ok(Value::Boolean(left_value > right_value)),
                &Operator::GtEq => Ok(Value::Boolean(left_value >= right_value)),
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

    // create simple filter expression for "id = 2"
    let filter_expr = Expr::BinaryExpr {
        left: Box::new(Expr::TupleValue(0)),
        op: Operator::Eq,
        right: Box::new(Expr::Literal(Value::UnsignedLong(2)))
    };

    println!("Expression: {:?}", filter_expr);

    // execute scan with filter expr against csv file
    let mut rdr = csv::Reader::from_file("people.csv").unwrap();
    let mut records = rdr.records();

    // <Option<Result<Vec<String>>>
    while let Some(row) = records.next() {
        let data : Vec<String> = row.unwrap();
        println!("Row: {:?}", data);

        // for now, do an expensive translation of strings to the specific tuple type
        let mut converted : Vec<Value> = vec![];

        for i in 0..data.len() {
            converted.push(match tt.columns[i].data_type {
                    DataType::UnsignedLong => Value::UnsignedLong(data[i].parse::<u64>().unwrap()),
                    DataType::String => Value::String(data[i].clone()),
            });
        }

        let tuple = SimpleTuple { values: converted };

        let is_match = evaluate(&tuple, &tt, &filter_expr).unwrap();
        println!("filter expr evaluates to {:?}", is_match);
    }

}
