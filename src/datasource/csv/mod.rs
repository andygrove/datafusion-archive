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

use std::io::{BufReader, BufWriter, Error};
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

                //TODO: interim code to map each row to a single row batch ... fix this later so we have
                // real batches
                let columns : Vec<ColumnData> = self.create_row(&record).unwrap()
                    .iter().map(|v| vec![v.clone()])
                    .collect();

                let batch: Box<Batch> = Box::new(ColumnBatch { columns });
                Ok(batch)
            },
            Err(e) => Err(ExecutionError::CsvError(e))
        });

        // real batches
        Box::new(batch_iter)
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }

}


