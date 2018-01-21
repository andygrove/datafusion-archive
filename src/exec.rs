use std::io::{Error, ErrorKind};
use std::io::{BufReader, BufRead};
use std::io::prelude::*;
use std::iter::Iterator;
use std::fs::File;
use std::path::Path;
use std::string::String;
use std::convert::*;
use exec::csv::StringRecords;


extern crate csv;
//use super::csv::StringRecords;

use super::rel::*;
use super::csvrelation::CsvRelation;

enum ExecutionError {
    IoError(Error),
    Custom(String)
}

impl From<Error> for ExecutionError {
    fn from(e: Error) -> Self {
        ExecutionError::IoError(e)
    }
}

trait SimpleRelation {
    fn next(&mut self) -> Result<Option<&Tuple>, ExecutionError>;
}

struct InMemoryRelation {
    values: Vec<Tuple>,
    index: usize
}

impl SimpleRelation for InMemoryRelation {
    fn next(&mut self) -> Result<Option<&Tuple>, ExecutionError> {
        if self.index < self.values.len() {
            let i = self.index;
            self.index += 1;
            Ok(Some(&self.values[i]))
        } else {
            Ok(None)
        }
    }
}

struct FileRelation {
    reader: csv::Reader<File>,
}

impl FileRelation {

    fn new(filename: &str, schema: TupleType) -> Result<Self, ExecutionError> {
        match csv::Reader::from_file(&filename) {
            Ok(mut r) => {

                // map the csv iterator into an iterator over tuples
                let iter : Iterator<Item=Result<Tuple,ExecutionError>> = r.records()
                    .map(|r| match r {
                        Ok(v) => Ok(FileRelation::create_tuple(&v, &schema)),
                        Err(_) => Err(ExecutionError::Custom("CSV error".to_string())) //TODO: improve error handling
                    });

                //TODO: store the iterator in the returned FileRelation

                Ok(FileRelation { reader: r })
            },
            Err(_) => panic!("oops") //TODO
        }
    }

    fn create_tuple(v: &Vec<String>, schema: &TupleType) -> Result<Tuple,ExecutionError> {
        //TODO: re-implement this in a more functional way
//        v.map(|r| r.map( |s| {
//
//        })
//                let mut converted : Vec<Value> = vec![];
//                //TODO: should be able to use zip() instead of a loop
//                for i in 0..r.len() {
//                    converted.push(match schema.columns[i].data_type {
//                        DataType::UnsignedLong => Value::UnsignedLong(v[i].parse::<u64>().unwrap()),
//                        DataType::String => Value::String(v[i].clone()),
//                        DataType::Double => Value::Double(v[i].parse::<f64>().unwrap()),
//                    });
//                }
//                Ok(Tuple { values: converted })
//            },
//            Err =>
//        }
        unimplemented!()
    }
}



impl SimpleRelation for FileRelation {
    fn next(&mut self) -> Result<Option<&Tuple>, ExecutionError> {

//        self.reader.read_
        unimplemented!()
    }
}



fn execute(plan: &Rel) -> Result<Box<Relation>,String> {
    match plan {
        &Rel::CsvFile { ref filename, ref schema } =>
            Ok(Box::new(CsvRelation::open(filename.to_string(), schema.clone()))),

        &Rel::Selection { ref expr, ref input } => {
//            let input_rel = execute(&input)?;
//            Ok(Box::new(FilterRelation {
//                input: input_rel,
//                schema: input_rel.schema().clone()
//            }))
                unimplemented!("selection")
        },

        _ => Err("not implemented".to_string())
    }
}

struct FilterRelation<'a> {
    schema: TupleType,
    input: Box<Relation<'a>>
}

impl<'a> Relation<'a> for FilterRelation<'a> {
    fn schema(&'a self) -> TupleType {
        self.schema.clone()
    }

    fn scan(&'a mut self) -> Box<Iterator<Item=Result<Tuple,String>> + 'a> {
        unimplemented!()
    }
}

/// Evaluate a relational expression against a tuple
pub fn evaluate(tuple: &Tuple, tt: &TupleType, rex: &Rex) -> Result<Value, Box<Error>> {

    match rex {
        &Rex::BinaryExpr { box ref left, ref op, box ref right } => {
            let left_value = evaluate(tuple, tt, left)?;
            let right_value = evaluate(tuple, tt, right)?;
            match op {
                &Operator::Eq => Ok(Value::Boolean(left_value == right_value)),
                &Operator::NotEq => Ok(Value::Boolean(left_value != right_value)),
                &Operator::Lt => Ok(Value::Boolean(left_value < right_value)),
                &Operator::LtEq => Ok(Value::Boolean(left_value <= right_value)),
                &Operator::Gt => Ok(Value::Boolean(left_value > right_value)),
                &Operator::GtEq => Ok(Value::Boolean(left_value >= right_value)),
            }
        },
        &Rex::TupleValue(index) => Ok(tuple.values[index].clone()),
        &Rex::Literal(ref value) => Ok(value.clone()),
    }

}

