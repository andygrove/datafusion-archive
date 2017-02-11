#![feature(box_patterns)]

use std::error::Error;
use std::iter::Iterator;
use std::string::String;
use std::fs::File;

use super::schema::*;

extern crate csv;
use self::csv::{Reader, StringRecords};

struct CsvRelation<'a> {
    filename: String,
    tuple_type: TupleType,
    reader: &'a csv::Reader<File>,
}

impl<'a> CsvRelation<'a> {

    fn open(filename: String, tuple_type: TupleType) -> Self {
        let rdr = csv::Reader::from_file(&filename).unwrap();
        CsvRelation {
            filename: filename,
            tuple_type: tuple_type,
            reader: &rdr,
        }
    }

}

fn create_tuple(v: Vec<String>, types: &Vec<DataType>) -> Tuple {
    let mut converted : Vec<Value> = vec![];
    //TODO: should be able to use zip() instead of a loop
    for i in 0..v.len() {
        converted.push(match types[i] {
            DataType::UnsignedLong => Value::UnsignedLong(v[i].parse::<u64>().unwrap()),
            DataType::String => Value::String(v[i].clone()),
        });
    }
    Tuple { values: converted }
}

impl<'a> Relation for CsvRelation<'a> {

    fn scan(&mut self) -> Box<Iterator<Item=Tuple>> {

        let types : Vec<DataType> = self.tuple_type
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        let records = &self.reader.records();

        let tuple_iter = records.map(|x| match x {
            Ok(v) => create_tuple(v, &types),
            Err(_) => Tuple { values: vec![] }
        });

        Box::new(tuple_iter)
    }

}
