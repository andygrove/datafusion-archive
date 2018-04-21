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

//! Defines data sources supported by DataFusion (currently CSV and Apache Parquet)

use std::fs::File;
use std::rc::Rc;

use arrow::array::*;
use arrow::builder::*;
use arrow::datatypes::*;

use parquet::basic;
use parquet::column::reader::*;
use parquet::data_type::ByteArray;
use parquet::file::reader::*;
use parquet::schema::types::Type;

use super::super::errors::*;
use super::super::types::*;
use super::common::*;

pub struct ParquetFile {
    reader: SerializedFileReader,
    row_index: usize,
    schema: Rc<Schema>,
    batch_size: usize,
}

impl ParquetFile {
    pub fn open(file: File) -> Result<Self> {
        let reader = SerializedFileReader::new(file).unwrap();

        let metadata = reader.metadata();
        let file_type = ParquetFile::to_arrow(metadata.file_metadata().schema());

        match file_type.data_type() {
            DataType::Struct(fields) => {
                let schema = Schema::new(fields.clone());
                //println!("Parquet schema: {:?}", schema);
                Ok(ParquetFile {
                    reader: reader,
                    row_index: 0,
                    schema: Rc::new(schema),
                    batch_size: 1024,
                })
            }
            _ => Err(ExecutionError::General(
                "Failed to read Parquet schema".to_string(),
            )),
        }
    }

    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size
    }

    fn to_arrow(t: &Type) -> Field {
        match t {
            Type::PrimitiveType {
                basic_info,
                physical_type,
                ..
                //type_length,
                //scale,
                //precision,
            } => {
//                println!("basic_info: {:?}", basic_info);

                let arrow_type = match physical_type {
                    basic::Type::BOOLEAN => DataType::Boolean,
                    basic::Type::INT32 => DataType::Int32,
                    basic::Type::INT64 => DataType::Int64,
                    basic::Type::INT96 => unimplemented!("No support for Parquet INT96 yet"),
                    basic::Type::FLOAT => DataType::Float32,
                    basic::Type::DOUBLE => DataType::Float64,
                    basic::Type::BYTE_ARRAY => match basic_info.logical_type() {
                        basic::LogicalType::UTF8 => DataType::Utf8,
                        _ => unimplemented!("No support for Parquet BYTE_ARRAY yet"),
                    }
                    basic::Type::FIXED_LEN_BYTE_ARRAY => unimplemented!("No support for Parquet FIXED_LEN_BYTE_ARRAY yet")
                };

                Field::new(basic_info.name(), arrow_type, false)
            }
            Type::GroupType { basic_info, fields } => {
                Field::new(
                basic_info.name(),
                DataType::Struct(
                    fields.iter().map(|f| ParquetFile::to_arrow(f)).collect()
                ),
                false)
            }
        }
    }
}

impl DataSource for ParquetFile {
    fn next(&mut self) -> Option<Result<Rc<RecordBatch>>> {
        if self.row_index < self.reader.num_row_groups() {
            match self.reader.get_row_group(self.row_index) {
                Err(_) => Some(Err(ExecutionError::General(
                    "parquet reader error".to_string(),
                ))),
                Ok(row_group_reader) => {
                    self.row_index += 1;

                    let mut arrays: Vec<Rc<Value>> =
                        Vec::with_capacity(row_group_reader.num_columns());
                    let mut row_count = 0;

                    for i in 0..row_group_reader.num_columns() {
                        let array: Option<Array> = match row_group_reader.get_column_reader(i) {
                            Ok(ColumnReader::ByteArrayColumnReader(ref mut r)) => {
                                let mut builder = vec![ByteArray::default(); 1024];
                                match r.read_batch(self.batch_size, None, None, &mut builder) {
                                    Ok((count, _)) => {
                                        row_count = count;
                                        //TODO: there is probably a more efficient way to copy the data to Arrow
                                        let strings: Vec<
                                            String,
                                        > = (builder[0..count])
                                            .iter()
                                            .map(|b| {
                                                String::from_utf8(
                                                    b.slice(0, b.len()).data().to_vec(),
                                                ).unwrap()
                                            })
                                            .collect::<Vec<String>>();
                                        Some(Array::from(strings))
                                    }
                                    _ => panic!("Error reading parquet batch (column {})", i),
                                }
                            }
                            Ok(ColumnReader::Int32ColumnReader(ref mut r)) => {
                                let mut builder: Builder<i32> =
                                    Builder::with_capacity(self.batch_size);
                                match r.read_batch(
                                    self.batch_size,
                                    None,
                                    None,
                                    builder.slice_mut(0, self.batch_size),
                                ) {
                                    Ok((count, _)) => {
                                        row_count = count;
                                        builder.set_len(count);
                                        Some(Array::from(builder.finish()))
                                    }
                                    _ => panic!("Error reading parquet batch (column {})", i),
                                }
                            }
                            Ok(ColumnReader::FloatColumnReader(ref mut r)) => {
                                let mut builder: Builder<f32> =
                                    Builder::with_capacity(self.batch_size);
                                match r.read_batch(
                                    self.batch_size,
                                    None,
                                    None,
                                    builder.slice_mut(0, self.batch_size),
                                ) {
                                    Ok((count, _)) => {
                                        row_count = count;
                                        builder.set_len(count);
                                        Some(Array::from(builder.finish()))
                                    }
                                    _ => panic!("Error reading parquet batch (column {})", i),
                                }
                            }
                            _ => {
                                println!("column type not supported");
                                None
                            }
                        };

                        if let Some(a) = array {
                            arrays.push(Rc::new(Value::Column(Rc::new(a))));
                        }
                    }

                    Some(Ok(Rc::new(DefaultRecordBatch {
                        schema: self.schema.clone(),
                        data: arrays,
                        row_count,
                    })))
                }
            }
        } else {
            None
        }
    }

    fn schema(&self) -> &Rc<Schema> {
        &self.schema
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn test_parquet() {
        let file = File::open("test/data/uk_cities.parquet").unwrap();
        let mut parquet = ParquetFile::open(file).unwrap();
        let batch = parquet.next().unwrap().unwrap();
        println!("Schema: {:?}", batch.schema());
        println!("rows: {}; cols: {}", batch.num_rows(), batch.num_columns());

        println!("First row: {:?}", batch.row_slice(0));
    }

    #[test]
    fn test_parquet_iterator() {
        let file = File::open("test/data/uk_cities.parquet").unwrap();
        let mut parquet = ParquetFile::open(file).unwrap();
        parquet.set_batch_size(2);
        let it = DataSourceIterator::new(Rc::new(RefCell::new(parquet)));
        it.for_each(|record_batch| match record_batch {
            Ok(b) => println!("new batch with {} rows", b.num_rows()),
            _ => println!("error"),
        });
    }
}
