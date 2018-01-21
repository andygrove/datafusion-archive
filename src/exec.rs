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

#[derive(Debug)]
pub enum ExecutionError {
    IoError(Error),
    Custom(String)
}

impl From<Error> for ExecutionError {
    fn from(e: Error) -> Self {
        ExecutionError::IoError(e)
    }
}

//#[derive(Debug)]
//struct InMemoryRelation<'a> {
//    tuples: &'a Vec<Tuple>,
//    schema: &'a TupleType
//}

/// Represents a csv file with a known schema
#[derive(Debug)]
pub struct CsvRelation {
    file: File,
    schema: TupleType
}

pub struct FilterRelation {
    schema: TupleType,
    input: Box<SimpleRelation>,
    expr: Rex
}

impl<'a> CsvRelation {

    pub fn open(file: File, schema: TupleType) -> Result<Self,ExecutionError> {
        Ok(CsvRelation { file, schema })
    }

    /// Convert StringRecord into our internal tuple type based on the known schema
    fn create_tuple(&self, r: &StringRecord) -> Result<Tuple,ExecutionError> {
        assert_eq!(self.schema.columns.len(), r.len());
        let values = self.schema.columns.iter().zip(r.into_iter()).map(|(c,s)| match c.data_type {
            //TODO: remove unwrap use here
            DataType::UnsignedLong => Value::UnsignedLong(s.parse::<u64>().unwrap()),
            DataType::String => Value::String(s.to_string()),
            DataType::Double => Value::Double(s.parse::<f64>().unwrap()),
        }).collect();
        Ok(Tuple::new(values))
    }
}

pub trait SimpleRelation {

    /// scan all records in this relation
    fn scan<'a>(&'a self) -> Box<Iterator<Item=Result<Tuple,ExecutionError>> + 'a>;

    //fn schema(&'a self) -> &'a TupleType;
}

impl SimpleRelation for CsvRelation {

    fn scan<'a>(&'a self) -> Box<Iterator<Item=Result<Tuple,ExecutionError>> + 'a> {

        //let CsvRelation { file, schema } = *self;

        let buf_reader = BufReader::new(&self.file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        let record_iter = csv_reader.into_records();

        let tuple_iter = record_iter.map(move|r| match r {
            Ok(record) => self.create_tuple(&record),
            Err(_) => Err(ExecutionError::Custom("TODO".to_string()))
        });

        Box::new(tuple_iter)
    }

//    fn schema(&'a self) -> &'a TupleType {
//        &self.schema
//    }

}

//impl<'a> SimpleRelation<'a> for InMemoryRelation<'a> {
//
//    fn scan<'a>(&'a self) -> Box<Iterator<Item=Result<Tuple,ExecutionError>> + 'a> {
//        let tuple_iter = self.tuples.iter().map(move |t| Ok(t.clone()));
//        Box::new(tuple_iter)
//    }
//
////    fn schema(&'a self) -> &'a TupleType {
////        &self.schema
////    }
//
//}


impl SimpleRelation for FilterRelation {

    fn scan<'a>(&'a self) -> Box<Iterator<Item=Result<Tuple, ExecutionError>> + 'a> {
        Box::new(self.input.scan().filter(|t| {
            //let x = evaluate(t, &self.schema, &self.expr);
            //TODO:
            true
        }))
    }

}



pub fn create_execution_plan<'a>(plan: &'a Rel) -> Result<Box<SimpleRelation + 'a>,ExecutionError> {
    match *plan {

        Rel::CsvFile { ref filename, ref schema } => {
            let file = File::open(filename)?;
            let rel = CsvRelation::open(file, schema.clone())?;
            Ok(Box::new(rel))
        },

//        Rel::Selection { expr, ref input, schema } => {
//            let input_rel = create_execution_plan(input)?;
//            Ok(Box::new(FilterRelation {
//                input: input_rel,
//                expr: expr,
//                schema: schema
//            }))
//        },

        _ => Err(ExecutionError::Custom("not implemented".to_string()))
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

