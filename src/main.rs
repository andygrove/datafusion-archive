#![feature(box_patterns)]

use std::error::Error;
use std::fs::File;

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

fn print_tuple(tuple_type: &TupleType, tuple: &Tuple) {
    for i in 0..tuple_type.columns.len() {
        print!("{:?}", tuple.get_value(i));
    }
    println!();
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
    TableScan { schema: String, table: String },
    Filter { expr: Expr, input: Box<PlanNode> },
    Project { expr: Vec<Expr>, input: Box<PlanNode> }
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

trait TupleConsumer {
    fn process(&self, tuple: &Tuple);
}

struct DebugConsumer {
    tuple_type: TupleType,
}

impl TupleConsumer for DebugConsumer {
    fn process(&self, tuple: &Tuple) {
        print_tuple(&self.tuple_type, tuple);
    }
}

struct FilterConsumer<'a> {
    tuple_type: TupleType,
    filter_expr: Expr,
    next_consumer: Option<&'a TupleConsumer>
}

impl<'a> TupleConsumer for FilterConsumer<'a> {
    fn process(&self, tuple: &Tuple) {
        match evaluate(tuple, &self.tuple_type, &self.filter_expr) {
            Ok(v) => match v {
                Value::Boolean(b) => {
                    if (b) {
                        match self.next_consumer {
                            Some(c) => c.process(tuple),
                            None => {}
                        }
                    }
                },
                //TODO: this should be an error - filter expressions should return boolean
                _ => {}
            },
            _ => {}
        }
    }
}

trait Relation {
    fn scan(&mut self, &TupleConsumer);
}

struct CsvRelation {
    filename: String,
    tuple_type: TupleType,
    reader: csv::Reader<File>,
}

impl CsvRelation {
    fn open(filename: String, tuple_type: TupleType) -> Self {
        let rdr = csv::Reader::from_file(&filename).unwrap();
        CsvRelation {
            filename: filename,
            tuple_type: tuple_type,
            reader: rdr,
        }
    }
}

impl Relation for CsvRelation {

    fn scan(&mut self, consumer: &TupleConsumer) {
        // iterate over data
        let mut records = self.reader.records();
        while let Some(row) = records.next() {
            let data : Vec<String> = row.unwrap();

            // for now, do an expensive translation of strings to the specific tuple type for
            // every single column
            let mut converted : Vec<Value> = vec![];
            for i in 0..data.len() {
                converted.push(match self.tuple_type.columns[i].data_type {
                    DataType::UnsignedLong => Value::UnsignedLong(data[i].parse::<u64>().unwrap()),
                    DataType::String => Value::String(data[i].clone()),
                });
            }
            let tuple = SimpleTuple { values: converted };

            consumer.process(&tuple);

        }

    }

}

fn main() {

    // define schema for data source (csv file)
    let tt = TupleType {
        columns: vec![
            ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    // open csv file
    let mut csv = CsvRelation::open(String::from("people.csv"), tt.clone());

    // create simple filter expression for "id = 2"
    let filter_expr = Expr::BinaryExpr {
        left: Box::new(Expr::TupleValue(0)),
        op: Operator::Eq,
        right: Box::new(Expr::Literal(Value::UnsignedLong(2)))
    };

    let debug_consumer = DebugConsumer { tuple_type: tt.clone() };

    let filter_consumer = FilterConsumer {
        tuple_type: tt.clone(),
        filter_expr: filter_expr,
        next_consumer: Some(&debug_consumer)
    };

    csv.scan(&filter_consumer);
}
