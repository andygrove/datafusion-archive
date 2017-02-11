#![feature(box_patterns)]

use std::error::Error;
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

impl Relation for CsvRelation {

    fn scan(&mut self) -> Box<Iterator<Item=Tuple>> {
        panic!("not implemented")

        // iterate over data
//        let mut records = self.reader.records();
//        while let Some(row) = records.next() {
//            let data : Vec<String> = row.unwrap();
//
//            // for now, do an expensive translation of strings to the specific tuple type for
//            // every single column
//            let mut converted : Vec<Value> = vec![];
//            for i in 0..data.len() {
//                converted.push(match self.tuple_type.columns[i].data_type {
//                    DataType::UnsignedLong => Value::UnsignedLong(data[i].parse::<u64>().unwrap()),
//                    DataType::String => Value::String(data[i].clone()),
//                });
//            }
//            let tuple = SimpleTuple { values: converted };
//
//            consumer.process(&tuple);
//
//        }

    }

}
