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
use super::super::csv::{StringRecord, StringRecordsIter};

/// Represents a csv file with a known schema
pub struct CsvRelation {
    schema: Schema,
    iter: Box<Iterator<Item=Result<StringRecord, csv::Error>>>
}

impl CsvRelation {

    pub fn open(file: File, schema: Schema) -> Result<Self,ExecutionError> {
        let buf_reader = BufReader::with_capacity(8*1024*1024,file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        let record_iter = csv_reader.into_records();
        let iter : Box<Iterator<Item=Result<StringRecord, csv::Error>>> = Box::new(record_iter);
        Ok(CsvRelation { schema, iter})
    }

}

impl SimpleRelation for CsvRelation {

    fn scan<'a>(&'a mut self, _ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Box<Batch>,ExecutionError>> + 'a> {
        let batch_iter: Box<Iterator<Item=Result<Box<Batch>,ExecutionError>>> = Box::new(CsvIterator { schema: &self.schema, iter: &mut self.iter });
        batch_iter
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

        println!("CsvIterator::next()");

        let max_size = 1000;

        //TODO: this seems inefficient .. loading as rows then converting to columns

        let mut rows : Vec<Vec<Value>> = vec![];
        for _ in 0 .. max_size {
            match self.iter.next() {
                Some(r) => rows.push(self.parse_record(&r.unwrap())),
                None => break
            }
        }

        if rows.is_empty() {
            return None
        }

        let mut columns : Vec<ColumnData> = Vec::with_capacity(self.schema.columns.len());

        for i in 0 .. self.schema.columns.len() {
            match self.schema.columns[i].data_type {
                DataType::Float => {
                    columns.push(ColumnData::Float(
                        rows.iter().map(|row| match &row[i] {
                            &Value::Float(v) => v,
                            _ => panic!()
                        }).collect()))
                },
                DataType::Double => {
                    columns.push(ColumnData::Double(
                        rows.iter().map(|row| match &row[i] {
                            &Value::Double(v) => v,
                            _ => panic!()
                        }).collect()))
                },
                DataType::UnsignedLong => {
                    columns.push(ColumnData::UnsignedLong(
                        rows.iter().map(|row| match &row[i] {
                            &Value::UnsignedLong(v) => v,
                            _ => panic!()
                        }).collect()))
                },
                DataType::String => {
                    columns.push(ColumnData::String(
                        rows.iter().map(|row| match &row[i] {
                            &Value::String(ref v) => v.clone(),
                            _ => panic!()
                        }).collect()))
                },
                _ => unimplemented!()
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
        let csv_reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(buf_reader);
        let record_iter = csv_reader.into_records();

        let mut foo : Box<Iterator<Item=Result<StringRecord, csv::Error>>> = Box::new(record_iter);

        let mut it = CsvIterator { schema: &schema, iter: &mut foo };

        let batch : Box<Batch> = it.next().unwrap().unwrap();

        assert_eq!(3, batch.col_count());
        assert_eq!(37, batch.row_count());

        let row = batch.row_slice(0);
        assert_eq!(vec![
            Value::String("Elgin, Scotland, the UK".to_string()),
            Value::Double(57.653484),
            Value::Double(-3.335724)], row);

        let names = batch.column(0);
        println!("names: {:?}", names);

    }
}
