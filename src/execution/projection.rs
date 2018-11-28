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

use std::cell::RefCell;
use std::fs::File;
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::error::Result;
use super::expression::RuntimeExpr;
use super::relation::Relation;

pub struct ProjectRelation {
    schema: Arc<Schema>,
    input: Rc<RefCell<Relation>>,
    expr: Vec<RuntimeExpr>,
}

impl ProjectRelation {
    pub fn new(input: Rc<RefCell<Relation>>, expr: Vec<RuntimeExpr>, schema: Arc<Schema>) -> Self {
        ProjectRelation {
            input,
            expr,
            schema,
        }
    }
}

impl Relation for ProjectRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        //TODO: apply projection
        self.input.borrow_mut().next()
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
//    fn scan<'a>(&'a mut self) -> Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a> {
//        let project_expr = &self.expr;
//        let x = self.input.borrow();
//
//        let projection_iter = x.scan().map(move |r| match r {
//            Ok(ref batch) => {
//                let projected_columns: Result<Vec<ArrayRef>> = project_expr
//                    .iter()
//                    .map(|e| e.get_func()(batch.as_ref()))
//                    .collect();
//
//                let projected_batch: Rc<RecordBatch> = Rc::new(RecordBatch::new(
//                    Arc::new(Schema::empty()), projected_columns?));
//
//                Ok(projected_batch)
//            }
//            Err(_) => r,
//        });
//
//        Box::new(projection_iter)
//    }
//
//    fn schema<'a>(&'a self) -> &'a Schema {
//        self.schema.as_ref()
//    }
//}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::datasource::DataSource;
    use super::super::relation::DataSourceRelation;
    use arrow::csv;
    use arrow::datatypes::{Schema, Field, DataType};

    #[test]
    fn project_all_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false)
        ]));
        let file = File::open("test/data/people.csv").unwrap();
        let csv = Rc::new(RefCell::new(csv::Reader::new(file, schema.clone(), true, 1024, None)));
        let ds = csv as Rc<RefCell<DataSource>>;
        let relation = Rc::new(RefCell::new(DataSourceRelation::new(schema.clone(), ds)));

        let mut projection = ProjectRelation::new(relation, vec![], schema);
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(2, batch.num_columns());
    }

}