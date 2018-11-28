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

use std::rc::Rc;
use std::cell::RefCell;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::datasource::{DataSource, DataSourceIterator};
use super::error::Result;

/// trait for all relations (a relation is essentially just an iterator over rows with
/// a known schema)
pub trait Relation {
    /// scan all records in this relation
    fn scan<'a>(&'a mut self) -> Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a>;

    /// get the schema for this relation
    fn schema<'a>(&'a self) -> &'a Schema;
}

struct DataSourceRelation {
    schema: Schema,
    ds: Rc<RefCell<DataSource>>,
}

impl Relation for DataSourceRelation {

    fn scan<'a>(&'a mut self) -> Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a> {
        Box::new(DataSourceIterator::new(self.ds.clone()))
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}