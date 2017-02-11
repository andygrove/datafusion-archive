#![feature(box_patterns)]

use std::error::Error;
use std::iter::Iterator;
use std::string::String;
use std::fs::File;

use super::schema::*;

extern crate csv;
use self::csv::{Reader, StringRecords};

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

struct CsvIterator<'a> {
    current_tuple: &'a Tuple
}

impl<'a> Iterator for CsvIterator<'a> {

    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {

        None
    }

}

impl Relation for CsvRelation {

    fn scan(&mut self) -> Box<Iterator<Item=Tuple>> {

        let types : Vec<DataType> = self.tuple_type
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        let a = &self.reader.records().map(|x| match x {
            Ok(stringVec) => {
                let mut converted : Vec<Value> = vec![];
                for i in 0..stringVec.len() {
                    converted.push(match types[i] {
                        DataType::UnsignedLong => Value::UnsignedLong(stringVec[i].parse::<u64>().unwrap()),
                        DataType::String => Value::String(stringVec[i].clone()),
                    });
                }
                Tuple { values: converted }
            },
            Err(_) => Tuple { values: vec![] }
        });
        
        //.collect::<Vec<Tuple>>()

        //let c : &Vec<Tuple> = &a.collect();


        // iterate over data
        //Box::new(it.iter())

        panic!("not implemented")
    }

}
