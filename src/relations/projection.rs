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

//! Relational Projection

use std::rc::Rc;

use super::super::datasources::common::*;
use super::super::errors::*;
use super::super::exec::*;
use super::super::types::*;

use arrow::datatypes::*;

pub struct ProjectRelation {
    schema: Rc<Schema>,
    input: Box<SimpleRelation>,
    expr: Vec<RuntimeExpr>,
}

impl ProjectRelation {

    pub fn new(input: Box<SimpleRelation>, expr: Vec<RuntimeExpr>, schema: Rc<Schema>) -> Self {
        ProjectRelation { input, expr, schema }
    }
}

impl SimpleRelation for ProjectRelation {
    fn scan<'a>(&'a mut self) -> Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a> {
        let project_expr = &self.expr;

        let projection_iter = self.input.scan().map(move |r| match r {
            Ok(ref batch) => {

                let projected_columns: Result<Vec<Value>> =
                    project_expr.iter()
                        .map(|e| e.get_func()(batch.as_ref())).collect();

                let projected_batch: Rc<RecordBatch> = Rc::new(DefaultRecordBatch {
                    schema: Rc::new(Schema::empty()), //TODO
                    data: projected_columns?,
                    row_count: batch.num_rows(),
                });

                Ok(projected_batch)
            }
            Err(_) => r,
        });

        Box::new(projection_iter)
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        self.schema.as_ref()
    }
}
