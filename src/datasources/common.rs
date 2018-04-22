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

use std::cell::RefCell;
use std::rc::Rc;
use std::str;

use arrow::array::*;
use arrow::datatypes::*;

use super::super::errors::*;
use super::super::types::*;

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
        ArrayData::Boolean(ref v) => ScalarValue::Boolean(*v.get(index)),
        ArrayData::Float32(ref v) => ScalarValue::Float32(*v.get(index)),
        ArrayData::Float64(ref v) => ScalarValue::Float64(*v.get(index)),
        ArrayData::Int8(ref v) => ScalarValue::Int8(*v.get(index)),
        ArrayData::Int16(ref v) => ScalarValue::Int16(*v.get(index)),
        ArrayData::Int32(ref v) => ScalarValue::Int32(*v.get(index)),
        ArrayData::Int64(ref v) => ScalarValue::Int64(*v.get(index)),
        ArrayData::UInt8(ref v) => ScalarValue::UInt8(*v.get(index)),
        ArrayData::UInt16(ref v) => ScalarValue::UInt16(*v.get(index)),
        ArrayData::UInt32(ref v) => ScalarValue::UInt32(*v.get(index)),
        ArrayData::UInt64(ref v) => ScalarValue::UInt64(*v.get(index)),
        ArrayData::Utf8(ref data) => {
            ScalarValue::Utf8(Rc::new(String::from(str::from_utf8(data.slice(index)).unwrap())))
        }
        ArrayData::Struct(ref v) => {
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
    fn next(&mut self) -> Option<Result<Rc<RecordBatch>>>;
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
    type Item = Result<Rc<RecordBatch>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ds.borrow_mut().next()
    }
}
