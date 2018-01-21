use std::io::{Error, ErrorKind, Read};
use std::io::{BufReader, BufRead};
use std::io::prelude::*;
use std::iter::Iterator;
use std::fs::File;
use std::path::Path;
use std::string::String;
use std::convert::*;

extern crate csv; // TODO:why do I need to this here as well as top level lib.rs ??

use super::csv::StringRecord;

use super::rel::*;

enum ExecutionError {
    IoError(Error),
    Custom(String)
}

impl From<Error> for ExecutionError {
    fn from(e: Error) -> Self {
        ExecutionError::IoError(e)
    }
}

struct InMemoryRelation<'a> {
    tuples: &'a Vec<Tuple>
}

struct CsvRelation<'a> {
    iter: Box<Iterator<Item=Result<Tuple,ExecutionError>> + 'a>,
}

impl<'a> CsvRelation<'a> {

    fn open(file: &'a File, schema: &'a TupleType) -> Result<Self,ExecutionError> {

        let buf_reader = BufReader::new(file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        let record_iter = csv_reader.into_records();

        let tuple_iter : Box<Iterator<Item=Result<Tuple,ExecutionError>>> = Box::new(record_iter.map(move |r| match r {
            Ok(record) => CsvRelation::create_tuple(&record, schema),
            Err(_) => Err(ExecutionError::Custom("TODO".to_string()))
        }));

        Ok(CsvRelation { iter: tuple_iter })
    }


    fn create_tuple(r: &StringRecord, schema: &TupleType) -> Result<Tuple,ExecutionError> {
        assert_eq!(schema.columns.len(), r.len());

        let values = schema.columns.iter().zip(r.into_iter()).map(|(c,s)| match c.data_type {
            DataType::UnsignedLong => Value::UnsignedLong(s.parse::<u64>().unwrap()),
            DataType::String => Value::String(s.to_string()),
            DataType::Double => Value::Double(s.parse::<f64>().unwrap()),
        }).collect::<Vec<Value>>();

        Ok(Tuple::new(values))
    }
}

trait SimpleRelation<'a> {
    /// scan all records in this relation
    fn scan(&'a mut self) -> Box<Iterator<Item=Result<Tuple,ExecutionError>> + 'a>;
}

impl<'a> SimpleRelation<'a> for CsvRelation<'a> {

    fn scan(&'a mut self) -> Box<Iterator<Item=Result<Tuple,ExecutionError>> + 'a> {
        self.iter
//      ^^^^ cannot move out of borrowed content
    }
}

impl<'a> SimpleRelation<'a> for InMemoryRelation<'a> {

    fn scan(&'a mut self) -> Box<Iterator<Item=Result<Tuple,ExecutionError>> + 'a> {
        // transform trom Tuple to Result<Tuple,_>
//        let tuple_results = self.tuples.iter().map(move |t| Ok(t.clone()));
//        Box::new(tuple_results)
        unimplemented!()
    }

}




fn execute(plan: &Rel) -> Result<Box<Relation>,String> {
//    match plan {
//        &Rel::CsvFile { ref filename, ref schema } =>
//            Ok(Box::new(CsvRelation::open(filename.to_string(), schema.clone()))),
//
//        &Rel::Selection { ref expr, ref input } => {
////            let input_rel = execute(&input)?;
////            Ok(Box::new(FilterRelation {
////                input: input_rel,
////                schema: input_rel.schema().clone()
////            }))
//                unimplemented!("selection")
//        },
//
//        _ => Err("not implemented".to_string())
//    }
    unimplemented!()
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

