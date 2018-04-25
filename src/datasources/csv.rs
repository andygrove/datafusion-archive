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

//! CSV Support

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::rc::Rc;

use arrow::array::*;
use arrow::builder::*;
use arrow::datatypes::*;
use arrow::list_builder::ListBuilder;

use csv;
use csv::{StringRecord, StringRecordsIntoIter};

use super::super::errors::*;
use super::super::types::*;
use super::common::*;

pub struct CsvFile {
    schema: Rc<Schema>,
    projection: Option<Vec<usize>>,
    record_iter: StringRecordsIntoIter<BufReader<File>>,
    batch_size: usize,
}

impl CsvFile {
    pub fn open(file: File, schema: Rc<Schema>, has_headers: bool, projection: Option<Vec<usize>>) -> Result<Self> {
        let csv_reader = csv::ReaderBuilder::new()
            .has_headers(has_headers)
            .from_reader(BufReader::new(file));

        let record_iter = csv_reader.into_records();
        Ok(CsvFile {
            schema: schema.clone(),
            projection,
            record_iter,
            batch_size: 1024,
        })
    }

    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size
    }
}

/// Built an Arrow array from one column in a batch of CSV records
macro_rules! collect_column {
    ($ROWS:expr, $COL_INDEX:expr, $TY:ty, $LEN:expr, $DEFAULT_VALUE:expr) => {{
        let mut b: Builder<$TY> = Builder::with_capacity($LEN);
        for row_index in 0..$LEN {
            b.push(match $ROWS[row_index].get($COL_INDEX) {
                Some(s) => if s.len() == 0 {
                    $DEFAULT_VALUE
                } else {
                    match s.parse::<$TY>() {
                        Ok(v) => v,
                        Err(e) => panic!(
                            "Failed to parse value '{}' as {} at batch row {} column {}: {:?}",
                            s,
                            stringify!($TY),
                            row_index,
                            $COL_INDEX,
                            e
                        ),
                    }
                },
                None => panic!(
                    "CSV file missing value at batch  row {}, column {}",
                    row_index, $COL_INDEX
                ),
            })
        }
        Value::Column(Rc::new(Array::from(b.finish())))
    }};
}

impl DataSource for CsvFile {
    fn next(&mut self) -> Option<Result<Rc<RecordBatch>>> {
        // read a batch of rows into memory
        let mut rows: Vec<StringRecord> = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            match self.record_iter.next() {
                Some(Ok(r)) => {
                    rows.push(r);
                }
                _ => break,
            }
        }

        if rows.len() == 0 {
            return None;
        }

        let column_with_index = self.schema
            .columns()
            .iter()
            .enumerate();

        let projection = match self.projection {
            Some(ref v) => v.clone(),
            None => self.schema
                .columns()
                .iter()
                .enumerate().
                map(|(i,_)| i)
            .collect()
        };

        let columns: Vec<Value> = column_with_index
            .map(|(i, c)| {
                if projection.contains(&i) {
                    match c.data_type() {
                        DataType::Boolean => collect_column!(rows, i, bool, rows.len(), false),
                        DataType::Int8 => collect_column!(rows, i, i8, rows.len(), 0),
                        DataType::Int16 => collect_column!(rows, i, i16, rows.len(), 0),
                        DataType::Int32 => collect_column!(rows, i, i32, rows.len(), 0),
                        DataType::Int64 => collect_column!(rows, i, i64, rows.len(), 0),
                        DataType::UInt8 => collect_column!(rows, i, u8, rows.len(), 0),
                        DataType::UInt16 => collect_column!(rows, i, u16, rows.len(), 0),
                        DataType::UInt32 => collect_column!(rows, i, u32, rows.len(), 0),
                        DataType::UInt64 => collect_column!(rows, i, u64, rows.len(), 0),
                        DataType::Float16 => collect_column!(rows, i, f32, rows.len(), 0_f32),
                        DataType::Float32 => collect_column!(rows, i, f32, rows.len(), 0_f32),
                        DataType::Float64 => collect_column!(rows, i, f64, rows.len(), 0_f64),
                        DataType::Utf8 => {
                            let mut builder: ListBuilder<u8> = ListBuilder::with_capacity(rows.len());
                            rows.iter().for_each(|row| {
                                let s = row.get(i).unwrap_or("").to_string();
                                builder.push(s.as_bytes());
                            });
                            let buffer = builder.finish();
                            Value::Column(Rc::new(Array::new(
                                rows.len(),
                                ArrayData::Utf8(buffer),
                            )))
                        }
                        _ => unimplemented!("CSV does not support data type {:?}", c.data_type())
                    }
                } else {
                    // not in the projection
                    //println!("Not loading column {} at index {}", c.name(), i);
                    Value::Scalar(Rc::new(ScalarValue::Null))
                }
            })
            .collect();

        Some(Ok(Rc::new(DefaultRecordBatch {
            schema: self.schema.clone(),
            data: columns,
            row_count: rows.len(),
        })))
    }

    fn schema(&self) -> &Rc<Schema> {
        &self.schema
    }
}

pub struct CsvWriter {
    pub w: BufWriter<File>,
}

impl CsvWriter {
    pub fn write_scalar(&mut self, v: &ScalarValue) {
        self.write_bytes(format!("{:?}", v).as_bytes());
    }
    pub fn write_bool(&mut self, v: &bool) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }
    pub fn write_u8(&mut self, v: &u8) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }
    pub fn write_u16(&mut self, v: &u16) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }
    pub fn write_u32(&mut self, v: &u32) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }
    pub fn write_u64(&mut self, v: &u64) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }
    pub fn write_i8(&mut self, v: &i8) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }
    pub fn write_i16(&mut self, v: &i16) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }
    pub fn write_i32(&mut self, v: &i32) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }
    pub fn write_i64(&mut self, v: &i64) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }
    pub fn write_f32(&mut self, v: &f32) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }
    pub fn write_f64(&mut self, v: &f64) {
        self.w.write(format!("{}", *v).as_bytes()).unwrap();
    }

    pub fn write_bytes(&mut self, s: &[u8]) {
        self.w.write(s).unwrap();
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::cell::RefCell;

    #[test]
    fn test_csv() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = CsvFile::open(file, Rc::new(schema), false, None).unwrap();
        let batch = csv.next().unwrap().unwrap();
        println!("rows: {}; cols: {}", batch.num_rows(), batch.num_columns());
    }

    #[test]
    fn test_csv_iterator() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);
        let file = File::open("test/data/uk_cities.csv").unwrap();
        let mut csv = CsvFile::open(file, Rc::new(schema), false, None).unwrap();
        csv.set_batch_size(2);
        let it = DataSourceIterator::new(Rc::new(RefCell::new(csv)));
        it.for_each(|record_batch| match record_batch {
            Ok(b) => println!("new batch with {} rows", b.num_rows()),
            _ => println!("error"),
        });
    }
}
