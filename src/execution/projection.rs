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
        match self.input.borrow_mut().next()? {
            Some(batch) => {
                let projected_columns: Result<Vec<ArrayRef>> =
                    self.expr.iter().map(|e| e.get_func()(&batch)).collect();

                let projected_batch: RecordBatch =
                    RecordBatch::new(Arc::new(Schema::empty()), projected_columns?);

                Ok(Some(projected_batch))
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::logicalplan::Expr;
    use super::super::context::ExecutionContext;
    use super::super::datasource::{CsvDataSource, DataSource};
    use super::super::expression;
    use super::super::relation::DataSourceRelation;
    use super::*;
    use arrow::csv;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn project_all_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
        ]));
        let file = File::open("test/data/people.csv").unwrap();
        let arrow_csv_reader = csv::Reader::new(file, schema.clone(), true, 1024, None);
        let ds = CsvDataSource::new(schema.clone(), arrow_csv_reader);
        let relation = Rc::new(RefCell::new(DataSourceRelation::new(
            schema.clone(),
            Rc::new(RefCell::new(ds)),
        )));
        let context = Rc::new(ExecutionContext::new());

        let projection_expr =
            vec![expression::compile_expr(context, &Expr::Column(0), schema.as_ref()).unwrap()];

        let mut projection = ProjectRelation::new(relation, projection_expr, schema);
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(1, batch.num_columns());
    }

}
