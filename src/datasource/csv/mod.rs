// Copyright 2018 Grove Enterprises LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{BufReader};
use std::fs::File;

use super::super::rel::{Schema, DataType, Value};
use super::super::exec::*;

extern crate csv;
use super::super::csv::StringRecord;

/// Represents a csv file with a known schema
#[derive(Debug)]
pub struct CsvRelation {
    file: File,
    schema: Schema
}

impl<'a> CsvRelation {

    pub fn open(file: File, schema: Schema) -> Result<Self,ExecutionError> {
        Ok(CsvRelation { file, schema })
    }

    /// Convert StringRecord into our internal row type based on the known schema
    fn create_row(&self, r: &StringRecord) -> Result<Vec<Value>,ExecutionError> {
        assert_eq!(self.schema.columns.len(), r.len());
        let values = self.schema.columns.iter().zip(r.into_iter()).map(|(c,s)| match c.data_type {
            //TODO: remove unwrap use here
            DataType::UnsignedLong => Value::Long(s.parse::<i64>().unwrap()),
            DataType::String => Value::String(s.to_string()),
            DataType::Double => Value::Double(s.parse::<f64>().unwrap()),
            _ => panic!("csv unsupported type")
        }).collect();
        Ok(values)
    }
}

impl SimpleRelation for CsvRelation {

    fn scan<'a>(&'a self, _ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Box<Batch>,ExecutionError>> + 'a> {

        let buf_reader = BufReader::with_capacity(8*1024*1024,&self.file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        let record_iter = csv_reader.into_records();

        let batch_iter = record_iter.map(move|r| match r {
            Ok(record) => {

                unimplemented!()
//                //TODO: interim code to map each row to a single row batch ... fix this later so we have
//                // real batches
//                let columns : Vec<ColumnData> = self.create_row(&record).unwrap()
//                    .iter().map(|v| vec![v.clone()])
//                    .collect();
//
//                let batch: Box<Batch> = Box::new(ColumnBatch { columns });
//                Ok(batch)
            },
            Err(e) => Err(ExecutionError::Custom(format!("Error reading CSV: {:?}", e)))
        });

        // real batches
        Box::new(batch_iter)
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }

}


struct CsvIterator<'a> {
    schema: &'a Schema,
    iter: &'a mut Box<Iterator<Item=Result<StringRecord, csv::Error>>>,
}

impl<'a> CsvIterator<'a> {

    /// Convert StringRecord into our internal row type based on the known schema
    fn parse_record(&self, r: &StringRecord) -> Vec<Value> {
        assert_eq!(self.schema.columns.len(), r.len());
        let values = self.schema.columns.iter().zip(r.into_iter()).map(|(c,s)| match c.data_type {
            //TODO: remove unwrap use here
            DataType::UnsignedLong => Value::Long(s.parse::<i64>().unwrap()),
            DataType::String => Value::String(s.to_string()),
            DataType::Double => Value::Double(s.parse::<f64>().unwrap()),
            _ => panic!("csv unsupported type")
        }).collect();
        values
    }

}

impl<'a> Iterator for CsvIterator<'a> {
    type Item=Result<Box<Batch>, ExecutionError>;

    fn next(&mut self) -> Option<Self::Item> {

        let max_size = 1000;

        let mut columns : Vec<ColumnData> = vec![];
        self.schema.columns.iter().for_each(|col| {
            match col.data_type {
                DataType::Float => columns.push(ColumnData::Float(Vec::with_capacity(max_size))),
                DataType::Double => columns.push(ColumnData::Double(Vec::with_capacity(max_size))),
                _ => unimplemented!()
            }
        });

        for _ in 0 .. max_size {
            match self.iter.next() {
                Some(r) => {

                    // parse the values
//                    let values = self.parse_record(&r.unwrap());
//
//                    for i in 0 .. values.len() {
//                        match (columns[i], values[i]) {
//                            (ColumnData::Double(vec), Value::Double(val)) => vec[i] = val,
//                            _ => unimplemented!()
//                        }
//
//                    }

                    unimplemented!()

                },
                None => if columns[0].len() == 0 {
                    return None
                }
            }
        }

        Some(Ok(Box::new(ColumnBatch { columns })))
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::rel::*;

    #[test]
    fn load_batch() {

        let schema = Schema::new(vec![
            Field::new("city", DataType::String, false),
            Field::new("lat", DataType::Double, false),
            Field::new("lng", DataType::Double, false)]);

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let buf_reader = BufReader::with_capacity(8*1024*1024,file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        let record_iter = csv_reader.into_records();

        let foo : Box<Iterator<Item=Result<StringRecord, csv::Error>>> = Box::new(record_iter);

        let mut it = CsvIterator { schema: &schema, iter: &mut foo };

        let batch = it.next().unwrap();

    }
}
