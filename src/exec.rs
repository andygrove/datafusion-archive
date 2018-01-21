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

pub struct ProjectRelation {
    schema: TupleType,
    input: Box<SimpleRelation>,
    expr: Vec<Rex>
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

    fn schema<'a>(&'a self) -> &'a TupleType;
}

impl SimpleRelation for CsvRelation {

    fn scan<'a>(&'a self) -> Box<Iterator<Item=Result<Tuple,ExecutionError>> + 'a> {

        let buf_reader = BufReader::new(&self.file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        let record_iter = csv_reader.into_records();

        let tuple_iter = record_iter.map(move|r| match r {
            Ok(record) => self.create_tuple(&record),
            Err(_) => Err(ExecutionError::Custom("TODO".to_string()))
        });

        Box::new(tuple_iter)
    }

    fn schema<'a>(&'a self) -> &'a TupleType {
        &self.schema
    }

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
        Box::new(self.input.scan().filter(move|t|
            match t {
                &Ok(ref tuple) => {
                    let predicate_eval = evaluate(tuple, &self.schema, &self.expr);

                    match predicate_eval {
                        Ok(Value::Boolean(b)) => b,
                        _ => false //TODO: error handling
                    }
                },
                _ => false //TODO: error handling
            }
        ))
    }

    fn schema<'a>(&'a self) -> &'a TupleType {
        &self.schema
    }

}



impl SimpleRelation for ProjectRelation {

    fn scan<'a>(&'a self) -> Box<Iterator<Item=Result<Tuple, ExecutionError>> + 'a> {
        let foo = self.input.scan().map(|r| match r {
            Ok(tuple) => Ok(tuple),
            Err(e) => panic!() // TODO
        });

        Box::new(foo)
    }

    fn schema<'a>(&'a self) -> &'a TupleType {
        &self.schema
    }
}

pub fn create_execution_plan(plan: &Rel) -> Result<Box<SimpleRelation>,ExecutionError> {
    match *plan {

        Rel::TableScan { ref schema, ref table } => {
            // for now, SQL tables are csv files and we have to hard-code the schema until
            // we have a way to define the schema through SQL e.g. REGISTER TABLE ...
            let schema = TupleType {
                columns: vec![
                    ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
                    ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
                ]
            };
            let file = File::open(format!("test/{}.csv", table))?;
            let rel = CsvRelation::open(file, schema.clone())?;
            Ok(Box::new(rel))
        },

        Rel::CsvFile { ref filename, ref schema } => {
            let file = File::open(filename)?;
            let rel = CsvRelation::open(file, schema.clone())?;
            Ok(Box::new(rel))
        },

        Rel::Selection { ref expr, ref input, ref schema } => {
            let input_rel = create_execution_plan(input)?;
            let rel = FilterRelation {
                input: input_rel,
                expr: expr.clone(),
                schema: schema.clone()
            };
            Ok(Box::new(rel))
        },

        Rel::Projection { ref expr, ref input } => {
            match input {
                &Some(ref r) => {
                    let input_rel = create_execution_plan(&r)?;
                    let input_schema = input_rel.schema().clone();

                    let project_columns: Vec<ColumnMeta> = expr.iter().map(|e| {
                        match e {
                            &Rex::TupleValue(i) => input_schema.columns[i].clone(),
                            _ => unimplemented!()
                        }
                    }).collect();

                    let project_schema = TupleType { columns: project_columns };

                    let rel = ProjectRelation {
                        input: input_rel,
                        expr: expr.clone(),
                        schema: project_schema,

                    };

                    Ok(Box::new(rel))
                },
                _ => unimplemented!("Projection currently requires an input relation")
            }
        },

        _ => {
            println!("Not implemented: {:?}", plan);
            Err(ExecutionError::Custom("not implemented".to_string()))
        }
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

