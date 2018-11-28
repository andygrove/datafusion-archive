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

//! Execution of a projection

use std::rc::Rc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::error::Result;
use super::expression::RuntimeExpr;
use super::relation::Relation;
//use super::value::Value;

pub struct ProjectRelation {
    schema: Rc<Schema>,
    input: Box<Relation>,
    expr: Vec<RuntimeExpr>,
}

impl ProjectRelation {
    pub fn new(input: Box<Relation>, expr: Vec<RuntimeExpr>, schema: Rc<Schema>) -> Self {
        ProjectRelation {
            input,
            expr,
            schema,
        }
    }
}

impl Relation for ProjectRelation {
    fn scan<'a>(&'a mut self) -> Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a> {
//        let project_expr = &self.expr;
//
//        let projection_iter = self.input.scan().map(move |r| match r {
//            Ok(ref batch) => {
//                let projected_columns: Result<Vec<Value>> = project_expr
//                    .iter()
//                    .map(|e| e.get_func()(batch.as_ref()))
//                    .collect();
//
//                let projected_batch: Rc<RecordBatch> = Rc::new(RecordBatch::new(
//                    Rc::new(Schema::empty()), projected_columns?));
//
//                Ok(projected_batch)
//            }
//            Err(_) => r,
//        });
//
//        Box::new(projection_iter)

        unimplemented!()
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        self.schema.as_ref()
    }
}