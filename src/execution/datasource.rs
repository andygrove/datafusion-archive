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

//! Logical query plan

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow::csv;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::error::{ExecutionError, Result};

pub trait DataSource {
    fn schema(&self) -> &Arc<Schema>;
    fn next(&mut self) -> Result<Option<RecordBatch>>;
}

pub struct CsvDataSource {
    schema: Arc<Schema>,
    reader: csv::Reader,
}

impl CsvDataSource {
    pub fn new(schema: Arc<Schema>, reader: csv::Reader) -> Self {
        Self { schema, reader }
    }
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.reader.next() {
            None => Ok(None),
            Some(Ok(r)) => Ok(Some(r)),
            Some(Err(e)) => Err(ExecutionError::from(e)),
        }
    }
}

//pub struct DataSourceIterator {
//    pub ds: Rc<RefCell<DataSource>>,
//}
//
//impl DataSourceIterator {
//    pub fn new(ds: Rc<RefCell<DataSource>>) -> Self {
//        DataSourceIterator { ds }
//    }
//}
//
//impl Iterator for DataSourceIterator {
//    type Item = Result<Rc<RecordBatch>>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        self.ds.borrow_mut().next()
//    }
//}

#[derive(Serialize, Deserialize, Clone)]
pub enum DataSourceMeta {
    /// Represents a CSV file with a provided schema
    CsvFile {
        filename: String,
        schema: Rc<Schema>,
        has_header: bool,
        projection: Option<Vec<usize>>,
    },
    /// Represents a Parquet file that contains schema information
    ParquetFile {
        filename: String,
        schema: Rc<Schema>,
        projection: Option<Vec<usize>>,
    },
}
