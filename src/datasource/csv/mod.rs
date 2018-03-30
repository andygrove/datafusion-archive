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
use std::rc::Rc;

use super::super::arrow::array::*;
use super::super::arrow::datatypes::*;
use super::super::exec::*;
use super::super::rel::*;

//extern crate bytes;
//use self::bytes::*;

extern crate csv;
use super::super::csv::StringRecord;

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
    fn parse_record(&self, r: &StringRecord) -> Vec<ScalarValue> {
        assert_eq!(self.schema.columns.len(), r.len());
        let values = self.schema.columns.iter().zip(r.into_iter()).map(|(c,s)| match c.data_type {
            DataType::Boolean => ScalarValue::Boolean(s.parse::<bool>().unwrap()),
            DataType::Float32 => ScalarValue::Float32(s.parse::<f32>().unwrap()),
            DataType::Float64 => ScalarValue::Float64(s.parse::<f64>().unwrap()),
            DataType::Int32 => ScalarValue::Int32(s.parse::<i32>().unwrap()),
            DataType::Int64 => ScalarValue::Int64(s.parse::<i64>().unwrap()),
            DataType::Utf8 => ScalarValue::Utf8(s.to_string()),
            _ => panic!("csv unsupported type")
        }).collect();
        values
    }

}

impl<'a> Iterator for CsvIterator<'a> {
    type Item=Result<Box<Batch>, ExecutionError>;

    fn next(&mut self) -> Option<Self::Item> {

        //println!("CsvIterator::next()");

        let max_size = 1000;

        // this seems inefficient .. loading as rows then converting to columns but this
        // is a row-based data source so what can we do?

        let mut rows : Vec<Vec<ScalarValue>> = Vec::with_capacity(max_size);
        for _ in 0 .. max_size {
            match self.iter.next() {
                Some(r) => rows.push(self.parse_record(&r.unwrap())),
                None => break
            }
        }

        if rows.is_empty() {
            return None
        }

        let mut columns : Vec<Rc<Value>> = Vec::with_capacity(self.schema.columns.len());

        for i in 0 .. self.schema.columns.len() {

            let name = format!("col{}", i+1);
            let field = Field::new(&name, self.schema.columns[i].data_type.clone(), false);

            let values : Array = match self.schema.columns[i].data_type {
                DataType::Boolean => {
                    Array::from(
                        rows.iter().map(|row| match &row[i] {
                            &ScalarValue::Boolean(v) => v,
                            _ => panic!()
                        }).collect::<Vec<bool>>())
                },
                DataType::Float32 => {
                    Array::from(
                        rows.iter().map(|row| match &row[i] {
                            &ScalarValue::Float32(v) => v,
                            _ => panic!()
                        }).collect::<Vec<f32>>())
                },
                DataType::Float64 => {
                    Array::from(
                        rows.iter().map(|row| match &row[i] {
                            &ScalarValue::Float64(v) => v,
                            _ => panic!()
                        }).collect::<Vec<f64>>())
                },
                DataType::Int32 => {
                    Array::from(
                        rows.iter().map(|row| match &row[i] {
                            &ScalarValue::Int32(v) => v,
                            _ => panic!()
                        }).collect::<Vec<i32>>())
                },
                DataType::Int64 => {
                    Array::from(
                        rows.iter().map(|row| match &row[i] {
                            &ScalarValue::Int64(v) => v,
                            _ => panic!()
                        }).collect::<Vec<i64>>())
                },
                DataType::Utf8 => {
                    //TODO: this can be optimized to avoid creating strings once arrow stabilizes
                    Array::from(
                        rows.iter().map(|row| match &row[i] {
                            &ScalarValue::Utf8(v) => v.clone(),
                            _ => panic!()
                        }).collect::<Vec<String>>())

//                    let mut offsets : Vec<i32> = Vec::with_capacity(rows.len() + 1);
//                    let mut buf = BytesMut::with_capacity(rows.len() * 32);
//
//                    offsets.push(0_i32);
//
//                    rows.iter().for_each(|row| match &row[i] {
//                        &ScalarValue::Utf8(ref v) => {
//                            buf.put(v.as_bytes());
//                            offsets.push(buf.len() as i32);
//                        },
//                        _ => panic!()
//                    });
//
//                    let bytes: Bytes = buf.freeze();
//
//                    Array::new(rows.len(), ArrayData::Utf8(ListData { offsets, bytes }))
                },
                _ => unimplemented!()
            };

            columns.push(Rc::new(Value::Column(Rc::new(field), Rc::new(values))));
        }

        Some(Ok(Box::new(ColumnBatch { row_count: rows.len(), columns })))
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_batch() {

        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false)]);

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
            ScalarValue::Utf8("Elgin, Scotland, the UK".to_string()),
            ScalarValue::Float64(57.653484),
            ScalarValue::Float64(-3.335724)], row);

        let _names = batch.column(0);
        //println!("names: {:?}", names);

    }
}
