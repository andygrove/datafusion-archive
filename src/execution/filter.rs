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

//! Execution of a filter (predicate)

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::error::Result;
use super::expression::RuntimeExpr;
use super::relation::Relation;

pub struct FilterRelation {
    schema: Arc<Schema>,
    input: Rc<RefCell<Relation>>,
    expr: RuntimeExpr,
}

impl FilterRelation {
    pub fn new(input: Rc<RefCell<Relation>>, expr: RuntimeExpr, schema: Arc<Schema>) -> Self {
        Self {
            schema,
            input,
            expr,
        }
    }
}

impl Relation for FilterRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.input.borrow_mut().next()? {
            Some(batch) => {

//                let projected_columns: Result<Vec<ArrayRef>> =
//                    self.expr.iter().map(|e| e.get_func()(&batch)).collect();
//
//                let projected_batch: RecordBatch =
//                    RecordBatch::new(Arc::new(Schema::empty()), projected_columns?);
//
//                Ok(Some(projected_batch))

                unimplemented!()
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
