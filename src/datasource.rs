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

use std::cell::RefCell;
use std::fs::File;
use std::io::BufReader;
use std::rc::Rc;
use std::str;

use arrow::array::*;
use arrow::builder::*;
use arrow::datatypes::*;

use csv;
use csv::StringRecordsIntoIter;
use parquet::basic;
use parquet::column::reader::*;
use parquet::data_type::ByteArray;
use parquet::file::reader::*;
use parquet::schema::types::Type;
use parquet::util::memory::{ByteBuffer, ByteBufferPtr};

use super::types::*;

pub trait RecordBatch {
    fn schema(&self) -> &Rc<Schema>;
    fn num_columns(&self) -> usize;
    fn num_rows(&self) -> usize;
    fn column(&self, index: usize) -> &Value;
    fn columns(&self) -> &Vec<Rc<Value>>;

    /// Read one row from a record batch (very inefficient but handy for debugging)
    fn row_slice(&self, index: usize) -> Vec<Rc<ScalarValue>> {
        self.columns()
            .iter()
            .map(|c| match c.as_ref() {
                &Value::Scalar(ref v) => v.clone(),
                &Value::Column(ref v) => Rc::new(get_value(v, index)),
            })
            .collect()
    }
}

pub fn get_value(column: &Array, index: usize) -> ScalarValue {
    ////println!("get_value() index={}", index);
    let v = match column.data() {
        &ArrayData::Boolean(ref v) => ScalarValue::Boolean(*v.get(index)),
        &ArrayData::Float32(ref v) => ScalarValue::Float32(*v.get(index)),
        &ArrayData::Float64(ref v) => ScalarValue::Float64(*v.get(index)),
        &ArrayData::Int8(ref v) => ScalarValue::Int8(*v.get(index)),
        &ArrayData::Int16(ref v) => ScalarValue::Int16(*v.get(index)),
        &ArrayData::Int32(ref v) => ScalarValue::Int32(*v.get(index)),
        &ArrayData::Int64(ref v) => ScalarValue::Int64(*v.get(index)),
        &ArrayData::UInt8(ref v) => ScalarValue::UInt8(*v.get(index)),
        &ArrayData::UInt16(ref v) => ScalarValue::UInt16(*v.get(index)),
        &ArrayData::UInt32(ref v) => ScalarValue::UInt32(*v.get(index)),
        &ArrayData::UInt64(ref v) => ScalarValue::UInt64(*v.get(index)),
        &ArrayData::Utf8(ref data) => {
            ScalarValue::Utf8(String::from(str::from_utf8(data.slice(index)).unwrap()))
        }
        &ArrayData::Struct(ref v) => {
            // v is Vec<ArrayData>
            // each field has its own ArrayData e.g. lat, lon so we want to get a value from each (but it's recursive)
            //            //println!("get_value() complex value has {} fields", v.len());
            let fields = v.iter().map(|arr| get_value(&arr, index)).collect();
            ScalarValue::Struct(fields)
        }
    };
    //    //println!("get_value() index={} returned {:?}", index, v);
    v
}

//TODO: remove pub from fields
pub struct DefaultRecordBatch {
    pub schema: Rc<Schema>,
    pub data: Vec<Rc<Value>>,
    pub row_count: usize,
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

    fn column(&self, index: usize) -> &Value {
        &self.data[index]
    }

    fn columns(&self) -> &Vec<Rc<Value>> {
        &self.data
    }
}

pub trait DataSource {
    fn schema(&self) -> &Rc<Schema>;
    fn next(&mut self) -> Option<Result<Rc<RecordBatch>, ExecutionError>>;
}

pub struct DataSourceIterator {
    pub ds: Rc<RefCell<DataSource>>,
}

impl DataSourceIterator {
    pub fn new(ds: Rc<RefCell<DataSource>>) -> Self {
        DataSourceIterator { ds }
    }
}

impl Iterator for DataSourceIterator {
    type Item = Result<Rc<RecordBatch>, ExecutionError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ds.borrow_mut().next()
    }
}

pub struct CsvFile {
    schema: Rc<Schema>,
    record_iter: StringRecordsIntoIter<BufReader<File>>,
    batch_size: usize,
}

impl CsvFile {
    pub fn open(file: File, schema: Rc<Schema>) -> Result<Self, ExecutionError> {
        let buf_reader = BufReader::with_capacity(8 * 1024 * 1024, file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        let record_iter = csv_reader.into_records();
        Ok(CsvFile {
            schema: schema.clone(),
            record_iter,
            batch_size: 1024,
        })
    }

    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size
    }
}

impl DataSource for CsvFile {
    fn next(&mut self) -> Option<Result<Rc<RecordBatch>, ExecutionError>> {
        let mut rows: Vec<Vec<ScalarValue>> = Vec::with_capacity(self.batch_size);

        for _ in 0..self.batch_size {
            match self.record_iter.next() {
                Some(Ok(r)) => {
                    let values: Vec<ScalarValue> = self.schema
                        .columns
                        .iter()
                        .zip(r.into_iter())
                        .map(|(c, s)| match c.data_type {
                            DataType::Boolean => ScalarValue::Boolean(s.parse::<bool>().unwrap()),
                            DataType::Float16 => ScalarValue::Float32(s.parse::<f32>().unwrap()),
                            DataType::Float32 => ScalarValue::Float32(s.parse::<f32>().unwrap()),
                            DataType::Float64 => ScalarValue::Float64(s.parse::<f64>().unwrap()),
                            DataType::Int8 => ScalarValue::Int8(s.parse::<i8>().unwrap()),
                            DataType::Int16 => ScalarValue::Int16(s.parse::<i16>().unwrap()),
                            DataType::Int32 => ScalarValue::Int32(s.parse::<i32>().unwrap()),
                            DataType::Int64 => ScalarValue::Int64(s.parse::<i64>().unwrap()),
                            DataType::UInt8 => ScalarValue::UInt8(s.parse::<u8>().unwrap()),
                            DataType::UInt16 => ScalarValue::UInt16(s.parse::<u16>().unwrap()),
                            DataType::UInt32 => ScalarValue::UInt32(s.parse::<u32>().unwrap()),
                            DataType::UInt64 => ScalarValue::UInt64(s.parse::<u64>().unwrap()),
                            DataType::Utf8 => ScalarValue::Utf8(s.to_string()),
                            DataType::Struct(_) => ScalarValue::Null, // CSV files do not have Structs
                        })
                        .collect();

                    rows.push(values);
                }
                _ => break,
            }
        }

        if rows.len() == 0 {
            return None;
        }

        let mut columns: Vec<Rc<Value>> = Vec::with_capacity(self.schema.columns.len());

        for i in 0..self.schema.columns.len() {
            let array: Array = match self.schema.columns[i].data_type {
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

            columns.push(Rc::new(Value::Column(Rc::new(array))));
        }

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

pub struct ParquetFile {
    reader: SerializedFileReader,
    row_index: usize,
    schema: Rc<Schema>,
    batch_size: usize,
}

impl ParquetFile {
    pub fn open(file: File) -> Result<Self, ExecutionError> {
        let reader = SerializedFileReader::new(file).unwrap();

        let metadata = reader.metadata();
        let file_type = ParquetFile::to_arrow(metadata.file_metadata().schema());

        match file_type.data_type {
            DataType::Struct(fields) => {
                let schema = Schema::new(fields);
                //println!("Parquet schema: {:?}", schema);
                Ok(ParquetFile {
                    reader: reader,
                    row_index: 0,
                    schema: Rc::new(schema),
                    batch_size: 1024,
                })
            }
            _ => Err(ExecutionError::Custom(
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

                Field {
                    name: basic_info.name().to_string(),
                    data_type: arrow_type,
                    nullable: false,
                }
            }
            Type::GroupType { basic_info, fields } => Field {
                name: basic_info.name().to_string(),
                data_type: DataType::Struct(
                    fields.iter().map(|f| ParquetFile::to_arrow(f)).collect(),
                ),
                nullable: false,
            },
        }
    }
}

impl DataSource for ParquetFile {
    fn next(&mut self) -> Option<Result<Rc<RecordBatch>, ExecutionError>> {
        if self.row_index < self.reader.num_row_groups() {
            match self.reader.get_row_group(self.row_index) {
                Err(_) => Some(Err(ExecutionError::Custom(format!("parquet reader error")))),
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
                                    _ => panic!(),
                                }
                            }
                            Ok(ColumnReader::Int32ColumnReader(ref mut r)) => {
                                let mut builder: Builder<i32> =
                                    Builder::with_capacity(self.batch_size);
                                match r.read_batch(self.batch_size, None, None, unsafe {
                                    builder.slice_mut(0, self.batch_size)
                                }) {
                                    Ok((count, _)) => {
                                        row_count = count;
                                        builder.set_len(count);
                                        Some(Array::from(builder.finish()))
                                    }
                                    _ => panic!("error"),
                                }
                            }
                            Ok(ColumnReader::FloatColumnReader(ref mut r)) => {
                                let mut builder: Builder<f32> =
                                    Builder::with_capacity(self.batch_size);
                                match r.read_batch(self.batch_size, None, None, unsafe {
                                    builder.slice_mut(0, self.batch_size)
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

    #[test]
    fn test_csv() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let file = File::open("test/data/uk_cities.csv").unwrap();

        let mut csv = CsvFile::open(file, Rc::new(schema)).unwrap();
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
        let mut csv = CsvFile::open(file, Rc::new(schema)).unwrap();
        csv.set_batch_size(2);
        let it = DataSourceIterator::new(Rc::new(RefCell::new(csv)));
        it.for_each(|record_batch| match record_batch {
            Ok(b) => println!("new batch with {} rows", b.num_rows()),
            _ => println!("error"),
        });
    }

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
