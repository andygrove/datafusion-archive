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

use super::super::datasources::common::*;
use super::super::errors::*;
use super::super::exec::*;
use arrow::datatypes::Schema;

pub struct LimitRelation {
    schema: Rc<Schema>,
    input: Box<SimpleRelation>,
    /// Max number of rows to return
    limit: usize,
}

impl LimitRelation {
    pub fn new(schema: Rc<Schema>, input: Box<SimpleRelation>, limit: usize) -> Self {
        LimitRelation {
            schema,
            input,
            limit,
        }
    }
}

impl SimpleRelation for LimitRelation {
    fn scan<'a>(&'a mut self) -> Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a> {
        Box::new(
            LimitIterator {
                count: 0,
                limit: self.limit,
                inner: self.input.scan(),
            }
        )
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        self.schema.as_ref()
    }
}

/// `LimitIterator` returns batches while the limit has not been reached yet
struct LimitIterator<'a> {
    /// Current count of rows
    count: usize,

    /// Max number of rows to return
    limit: usize,

    /// Inner iterator where the record batches will be pulled from
    inner: Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a>,
}

impl<'a> Iterator for LimitIterator<'a> {
    type Item = Result<Rc<RecordBatch>>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.count >= self.limit {
            return None;
        }

        Some(match self.inner.next()? {
            Ok(ref b) => {
                if self.count + b.num_rows() < self.limit {
                    self.count += b.num_rows();
                    Ok(b.clone())
                } else {
                    let n = b.num_rows().min(self.limit - self.count);
                    self.count += n;
                    let new_batch: Rc<RecordBatch> = Rc::new(DefaultRecordBatch {
                        schema: b.schema().clone(),
                        data: b.columns().clone(),
                        row_count: n,
                    });
                    Ok(new_batch)
                }
            }
            Err(e) => Err(e),
        })
    }
}