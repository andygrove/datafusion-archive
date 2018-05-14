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

//! Limit Relation

use std::rc::Rc;

use arrow::datatypes::Schema;
use super::super::datasources::common::*;
use super::super::errors::*;
use super::super::exec::*;

pub struct LimitRelation {
    schema: Rc<Schema>,
    input: Box<SimpleRelation>,
    /// Max number of rows to return
    limit: usize,
}

impl LimitRelation {
    pub fn new(schema: Rc<Schema>,
               input: Box<SimpleRelation>,
               limit: usize) -> Self {
        LimitRelation { schema, input, limit }
    }
}

impl SimpleRelation for LimitRelation {

    fn scan<'a>(&'a mut self) -> Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a> {

        let mut count: usize = 0;
        let limit = self.limit;

        Box::new(self.input.scan().map(move|batch| {
            match batch {
                Ok(ref b) => {
                    if count + b.num_rows() < limit {
                        count += b.num_rows();
                        Ok(b.clone())
                    } else {
                        let n = b.num_rows().min(limit-count);
                        count += n;
                        let new_batch: Rc<RecordBatch> = Rc::new(DefaultRecordBatch {
                            schema: b.schema().clone(),
                            data: b.columns().clone(),
                            row_count: n,
                        });
                        Ok(new_batch)
                    }
                },
                Err(e) => Err(e)
            }
        }))
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        self.schema.as_ref()
    }
}



