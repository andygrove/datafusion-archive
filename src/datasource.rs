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

use std::fs::File;
use std::io::BufReader;
use std::rc::Rc;

use arrow::array::*;
use arrow::builder::*;
use arrow::datatypes::*;

use csv;
use csv::*;
use parquet::basic::LogicalType;
use parquet::column::reader::*;
use parquet::file::reader::*;
use parquet::schema::types::*;

use super::types::*;

trait RecordBatch {
    fn schema(&self) -> &Rc<Schema>;
    fn num_columns(&self) -> usize;
    fn num_rows(&self) -> usize;
    fn column(&self, index: usize) -> &Rc<Array>;
}

struct DefaultRecordBatch {
    schema: Rc<Schema>,
    data: Vec<Rc<Array>>,
    row_count: usize,
}

impl RecordBatch for DefaultRecordBatch {
    fn schema(&self) -> &Rc<Schema> {
        &self.schema
    }

    fn num_columns(&self) -> usize {
        self.data.len()
    }

    fn num_rows(&self) -> usize {
        self.row_count
    }

    fn column(&self, index: usize) -> &Rc<Array> {
        &self.data[index]
    }
}

trait DataSource {
    fn next(&mut self) -> Option<Box<RecordBatch>>;
}

pub struct CsvFile {
    schema: Rc<Schema>,
    record_iter: StringRecordsIntoIter<BufReader<File>>,
}

impl CsvFile {
    fn open(file: File, schema: Rc<Schema>) -> Self {
        let buf_reader = BufReader::with_capacity(8 * 1024 * 1024, file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        let record_iter = csv_reader.into_records();
        CsvFile {
            schema: schema.clone(),
            record_iter,
        }
    }
}

impl DataSource for CsvFile {
    fn next(&mut self) -> Option<Box<RecordBatch>> {
        let batch_size = 10;

        let mut rows: Vec<Vec<ScalarValue>> = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            match self.record_iter.next() {
                Some(Ok(r)) => {
                    let values: Vec<ScalarValue> = self.schema
                        .columns
                        .iter()
                        .zip(r.into_iter())
                        .map(|(c, s)| match c.data_type {
                            DataType::Boolean => ScalarValue::Boolean(s.parse::<bool>().unwrap()),
                            DataType::Float32 => ScalarValue::Float32(s.parse::<f32>().unwrap()),
                            DataType::Float64 => ScalarValue::Float64(s.parse::<f64>().unwrap()),
                            DataType::Int32 => ScalarValue::Int32(s.parse::<i32>().unwrap()),
                            DataType::Int64 => ScalarValue::Int64(s.parse::<i64>().unwrap()),
                            DataType::Utf8 => ScalarValue::Utf8(s.to_string()),
                            _ => panic!("csv unsupported type"),
                        })
                        .collect();

                    rows.push(values);
                }
                _ => break,
            }
        }

        let mut columns: Vec<Rc<Array>> = Vec::with_capacity(self.schema.columns.len());

        for i in 0..self.schema.columns.len() {
            let values: Array = match self.schema.columns[i].data_type {
                DataType::Boolean => Array::from(
                    rows.iter()
                        .map(|row| match &row[i] {
                            &ScalarValue::Boolean(v) => v,
                            _ => panic!(),
                        })
                        .collect::<Vec<bool>>(),
                ),
                DataType::Float32 => Array::from(
                    rows.iter()
                        .map(|row| match &row[i] {
                            &ScalarValue::Float32(v) => v,
                            _ => panic!(),
                        })
                        .collect::<Vec<f32>>(),
                ),
                DataType::Float64 => Array::from(
                    rows.iter()
                        .map(|row| match &row[i] {
                            &ScalarValue::Float64(v) => v,
                            _ => panic!(),
                        })
                        .collect::<Vec<f64>>(),
                ),
                DataType::Int32 => Array::from(
                    rows.iter()
                        .map(|row| match &row[i] {
                            &ScalarValue::Int32(v) => v,
                            _ => panic!(),
                        })
                        .collect::<Vec<i32>>(),
                ),
                DataType::Int64 => Array::from(
                    rows.iter()
                        .map(|row| match &row[i] {
                            &ScalarValue::Int64(v) => v,
                            _ => panic!(),
                        })
                        .collect::<Vec<i64>>(),
                ),
                DataType::Utf8 => {
                    //TODO: this can be optimized to avoid creating strings once arrow stabilizes
                    Array::from(
                        rows.iter()
                            .map(|row| match &row[i] {
                                &ScalarValue::Utf8(ref v) => v.clone(),
                                _ => panic!(),
                            })
                            .collect::<Vec<String>>(),
                    )

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
                }
                _ => unimplemented!(),
            };

            columns.push(Rc::new(values));
        }

        Some(Box::new(DefaultRecordBatch {
            schema: self.schema.clone(),
            data: columns,
            row_count: rows.len(),
        }) as Box<RecordBatch>)
    }
}

pub struct ParquetFile {
    reader: SerializedFileReader,
    row_index: usize,
}

impl ParquetFile {

    fn open(file: File) -> Self {
        ParquetFile {
            reader: SerializedFileReader::new(file).unwrap(),
            row_index: 0,
        }
    }

    fn to_arrow(t: &Type) -> Field {
        match t {
            Type::PrimitiveType { basic_info, physical_type, type_length, scale, precision } => {

                println!("basic_info: {:?}", basic_info);

                let arrow_type = match basic_info.logical_type() {
                    LogicalType::UINT_8 => DataType::UInt8,
                    LogicalType::UINT_16 => DataType::UInt16,
                    LogicalType::UINT_32 => DataType::UInt32,
                    LogicalType::UINT_64 => DataType::UInt64,
                    LogicalType::INT_8 => DataType::Int8,
                    LogicalType::INT_16 => DataType::Int16,
                    LogicalType::INT_32 => DataType::Int32,
                    LogicalType::INT_64 => DataType::Int64,
                    LogicalType::UTF8 => DataType::Utf8,
                    _ => {
                        println!("Unsupported parquet type {}", basic_info.logical_type());
                        DataType::Int32 //TODO
                    }

                    //TODO
                    /*
                    NONE,
                    MAP,
                    MAP_KEY_VALUE,
                    LIST,
                    ENUM,
                    DECIMAL,
                    DATE,
                    TIME_MILLIS,
                    TIME_MICROS,
                    TIMESTAMP_MILLIS,
                    TIMESTAMP_MICROS,
                    JSON,
                    BSON,
                    INTERVAL
                    */
                };

                Field {
                    name: basic_info.name().to_string(),
                    data_type: arrow_type,
                    nullable: false }

            },
            Type::GroupType { basic_info, fields } => {
                Field {
                    name:basic_info.name().to_string(),
                    data_type: DataType::Struct(fields.iter().map(|f| ParquetFile::to_arrow(f)).collect()),
                    nullable: false
                }
            }
        }

        /*
        #[derive(Debug, PartialEq)]
pub enum Type {
  PrimitiveType {
    basic_info: BasicTypeInfo, physical_type: PhysicalType,
    type_length: i32, scale: i32, precision: i32
  },
  GroupType {
    basic_info: BasicTypeInfo, fields: Vec<TypePtr>
  }
}
*/

    }
}

impl DataSource for ParquetFile {
    fn next(&mut self) -> Option<Box<RecordBatch>> {

        let metadata = self.reader.metadata();

        let file_type = ParquetFile::to_arrow(metadata.file_metadata().schema());

        let row_group_reader = self.reader.get_row_group(self.row_index).unwrap();

        let batch_size = 1024;

        let mut arrays: Vec<Rc<Array>> = Vec::with_capacity(row_group_reader.num_columns());
        let mut row_count = 0;

        for i in 0..row_group_reader.num_columns() {
            let array: Option<Array> = match row_group_reader.get_column_reader(i) {
                //TODO: support all column types
                Ok(ColumnReader::Int32ColumnReader(ref mut r)) => {
                    let mut builder: Builder<i32> = Builder::with_capacity(batch_size);
                    match r.read_batch(batch_size, None, None, unsafe {
                        builder.slice_mut(0, batch_size)
                    }) {
                        Ok((count, _)) => {
                            row_count = count;
                            builder.set_len(count);
                            Some(Array::from(builder.finish()))
                        }
                        _ => panic!("error"),
                    }
                }
                _ => {
                    println!("column type not supported");
                    None
                }
            };

            if let Some(a) = array {
                arrays.push(Rc::new(a));
            }
        }

        let schema = match file_type.data_type {
            DataType::Struct(fields) => Schema::new(fields),
            _ => panic!()
        };

        Some(Box::new(DefaultRecordBatch {
            schema: Rc::new(schema),
            data: arrays,
            row_count,
        }))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_csv() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = CsvFile::open(file, Rc::new(schema));
        let batch = csv.next().unwrap();
        println!("rows: {}; cols: {}", batch.num_rows(), batch.num_columns());
    }

    #[test]
    fn test_parquet() {
        let file = File::open("test/data/alltypes_plain.parquet").unwrap();
        let mut parquet = ParquetFile::open(file);
        let batch = parquet.next().unwrap();
        println!("Schema: {:?}", batch.schema());
        println!("rows: {}; cols: {}", batch.num_rows(), batch.num_columns());
    }
}
