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

//! ndjson (newline-delimited JSON) Support

use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::rc::Rc;

use arrow::array::ListArray;
use arrow::bitmap::*;
use arrow::builder::*;
use arrow::datatypes::{DataType, Schema};
use arrow::list_builder::ListBuilder;

use json;

use super::super::errors::*;
use super::super::types::*;
use super::common::*;

pub struct NdJsonFile {
    schema: Rc<Schema>,
    projection: Option<Vec<usize>>,
    lines: Box<Iterator<Item = io::Result<String>>>,
    batch_size: usize,
}

impl NdJsonFile {
    pub fn open(f: File, schema: Rc<Schema>, projection: Option<Vec<usize>>) -> Result<Self> {
        let reader = BufReader::new(f);
        let it = reader.lines();
        Ok(NdJsonFile {
            schema: schema.clone(),
            lines: Box::new(it),
            batch_size: 1024,
            projection,
        })
    }
}

/// Built an Arrow array from one column in a batch of JSON records
macro_rules! collect_column {
    ($ROWS:expr, $NAME:expr, $TY:ty, $ACCESSOR:ident, $LEN:expr, $DEFAULT_VALUE:expr) => {{
        let mut bitmap = Bitmap::new($LEN);
        let mut null_count = 0;
        let mut b: Builder<$TY> = Builder::with_capacity($LEN);
        for row_index in 0..$LEN {
            let field_name: &str = $NAME.as_ref();
            let json_value: &json::JsonValue = &$ROWS[row_index][field_name];
            if json_value.is_null() {
                null_count += 1;
                bitmap.clear(row_index);
                b.push($DEFAULT_VALUE)
            } else {
                b.push(json_value.$ACCESSOR().unwrap());
            }
        }
        let data = ArrayData::from(b.finish());
        Value::Column(Rc::new(Array::with_nulls($LEN, data, null_count, bitmap)))
    }};
}

impl DataSource for NdJsonFile {
    fn schema(&self) -> &Rc<Schema> {
        unimplemented!()
    }

    fn next(&mut self) -> Option<Result<Rc<RecordBatch>>> {
        // load a batch of JSON records into memory
        let mut rows: Vec<json::JsonValue> = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            match self.lines.next() {
                Some(Ok(r)) => {
                    rows.push(json::parse(&r).unwrap());
                }
                Some(Err(e)) => panic!("{:?}", e),
                None => break,
            }
        }

        if rows.len() == 0 {
            return None;
        }

        println!("Loaded {} rows", rows.len());

        // now convert to Arrow arrays based on provided schema

        let column_with_index = self.schema.columns().iter().enumerate();

        let projection = match self.projection {
            Some(ref v) => v.clone(),
            None => self
                .schema
                .columns()
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect(),
        };

        let columns: Vec<Value> = column_with_index
            .map(|(i, c)| {
                if projection.contains(&i) {
                    match c.data_type() {
                        DataType::Boolean => {
                            collect_column!(rows, c.name(), bool, as_bool, rows.len(), false)
                        }
                        DataType::Int8 => collect_column!(rows, c.name(), i8, as_i8, rows.len(), 0),
                        DataType::Int16 => {
                            collect_column!(rows, c.name(), i16, as_i16, rows.len(), 0)
                        }
                        DataType::Int32 => {
                            collect_column!(rows, c.name(), i32, as_i32, rows.len(), 0)
                        }
                        DataType::Int64 => {
                            collect_column!(rows, c.name(), i64, as_i64, rows.len(), 0)
                        }
                        DataType::UInt8 => {
                            collect_column!(rows, c.name(), u8, as_u8, rows.len(), 0)
                        }
                        DataType::UInt16 => {
                            collect_column!(rows, c.name(), u16, as_u16, rows.len(), 0)
                        }
                        DataType::UInt32 => {
                            collect_column!(rows, c.name(), u32, as_u32, rows.len(), 0)
                        }
                        DataType::UInt64 => {
                            collect_column!(rows, c.name(), u64, as_u64, rows.len(), 0)
                        }
                        DataType::Float16 => {
                            collect_column!(rows, c.name(), f32, as_f32, rows.len(), 0_f32)
                        }
                        DataType::Float32 => {
                            collect_column!(rows, c.name(), f32, as_f32, rows.len(), 0_f32)
                        }
                        DataType::Float64 => {
                            collect_column!(rows, c.name(), f64, as_f64, rows.len(), 0_f64)
                        }
                        DataType::Utf8 => {
                            let mut b: ListBuilder<u8> = ListBuilder::with_capacity(rows.len());
                            let mut bitmap = Bitmap::new(rows.len());
                            let mut null_count = 0;
                            for row_index in 0..rows.len() {
                                let field_name: &str = c.name().as_ref();
                                let json_value: &json::JsonValue = &rows[row_index][field_name];
                                if json_value.is_null() {
                                    null_count += 1;
                                    bitmap.clear(row_index);
                                    b.push(b"");
                                } else {
                                    b.push(json_value.as_str().unwrap().as_bytes());
                                }
                            }
                            let buffer = b.finish();
                            Value::Column(Rc::new(Array::with_nulls(
                                rows.len(),
                                ArrayData::Utf8(ListArray::from(buffer)),
                                null_count,
                                bitmap,
                            )))
                        }
                        _ => unimplemented!(
                            "ndjson reader does not support data type {:?}",
                            c.data_type()
                        ),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_read_simple_file() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Float64, false),
        ]);

        let file = File::open("test/data/example1.ndjson").unwrap();

        let mut file = NdJsonFile::open(file, Rc::new(schema), None).unwrap();
        let batch = file.next().unwrap().unwrap();
        assert_eq!(3, batch.num_rows());
        assert_eq!(3, batch.num_columns());
    }
}
