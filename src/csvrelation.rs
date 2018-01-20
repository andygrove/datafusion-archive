use std::iter::Iterator;
use std::string::String;
use std::fs::File;

use super::schema::*;
use super::rel::*;

extern crate csv;

pub struct CsvRelation {
    filename: String,
    tuple_type: TupleType,
    reader: csv::Reader<File>,
}

impl CsvRelation {

    pub fn open(filename: String, tuple_type: TupleType) -> Self {
        let rdr = csv::Reader::from_file(&filename).unwrap();
        CsvRelation {
            filename: filename,
            tuple_type: tuple_type,
            reader: rdr,
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
            DataType::Double => Value::Double(v[i].parse::<f64>().unwrap()),
        });
    }
    Tuple { values: converted }
}

impl<'a> Relation<'a> for CsvRelation {

    fn scan(&'a mut self) -> Box<Iterator<Item=Tuple> + 'a> {

        let types : Vec<DataType> = self.tuple_type
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        let records = self.reader.records();

        // create iterator over tuples
        let tuple_iter = records.map(move |x| match x {
            Ok(v) => create_tuple(v, &types),
            Err(_) => Tuple { values: vec![] } //TODO: real error handling
        });

        Box::new(tuple_iter)
    }

}
