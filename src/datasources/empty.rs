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

use super::super::errors::*;
use super::common::*;

use arrow::datatypes::*;

pub struct EmptyRelation {
    first: bool,
    schema: Rc<Schema>,
}

impl EmptyRelation {
    pub fn new() -> Self {
        EmptyRelation {
            first: true,
            schema: Rc::new(Schema::new(vec![])),
        }
    }
}

impl DataSource for EmptyRelation {
    fn schema(&self) -> &Rc<Schema> {
        &self.schema
    }

    fn next(&mut self) -> Option<Result<Rc<RecordBatch>>> {
        if self.first {
            self.first = false;
            Some(Ok(Rc::new(DefaultRecordBatch {
                schema: self.schema.clone(),
                data: Vec::new(),
                row_count: 1,
            })))
        } else {
            None
        }
    }
}
