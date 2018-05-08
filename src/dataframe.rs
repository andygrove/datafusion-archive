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

//! DataFrame functionality

use std::clone::Clone;
use std::rc::Rc;
use std::str;

use arrow::datatypes::*;

use super::errors::*;
use super::exec::*;
use super::logical::*;

/// DataFrame is an abstraction of a logical plan and a schema
pub trait DataFrame {
    /// Projection
    fn select(&self, expr: Vec<Expr>) -> Result<Rc<DataFrame>>;

    /// Selection
    fn filter(&self, expr: Expr) -> Result<Rc<DataFrame>>;

    /// Return an expression representing the specified column
    fn col(&self, column_name: &str) -> Result<Expr>;

    fn schema(&self) -> &Rc<Schema>;

    fn plan(&self) -> &Rc<LogicalPlan>;

    /// show N rows (useful for debugging)
    fn show(&self, count: usize);
}

pub struct DF {
    ctx: ExecutionContext,
    pub plan: Rc<LogicalPlan>,
}

impl DF {
    pub fn new(ctx: ExecutionContext, plan: Rc<LogicalPlan>) -> Self {
        DF { ctx, plan }
    }

    pub fn with_plan(&self, plan: Rc<LogicalPlan>) -> Self {
        DF::new(self.ctx.clone(), plan)
    }
}

impl DataFrame for DF {
    fn select(&self, expr: Vec<Expr>) -> Result<Rc<DataFrame>> {
        use sqlplanner::exprlist_to_fields;
        let projection_schema =
            Rc::new(Schema::new(exprlist_to_fields(&expr, self.schema())));

        let plan = LogicalPlan::Projection {
            expr: expr,
            input: self.plan.clone(),
            schema: projection_schema,
        };

        Ok(Rc::new(self.with_plan(Rc::new(plan))))
    }

//    fn sort(&self, expr: Vec<Expr>) -> Result<Rc<DataFrame>> {
//        let plan = LogicalPlan::Sort {
//            expr: expr,
//            input: self.plan.clone(),
//            schema: self.plan.schema().clone(),
//        };
//
//        Ok(Rc::new(self.with_plan(Rc::new(plan))))
//    }

    fn filter(&self, expr: Expr) -> Result<Rc<DataFrame>> {
        let plan = LogicalPlan::Selection {
            expr: expr,
            input: self.plan.clone(),
        };

        Ok(Rc::new(self.with_plan(Rc::new(plan))))
    }

    fn col(&self, column_name: &str) -> Result<Expr> {
        match self.plan.schema().column_with_name(column_name) {
            Some((i, _)) => Ok(Expr::Column(i)),
            _ => Err(ExecutionError::InvalidColumn(column_name.to_string())),
        }
    }

    fn schema(&self) -> &Rc<Schema> {
        self.plan.schema()
    }

    fn plan(&self) -> &Rc<LogicalPlan> {
        &self.plan
    }

    fn show(&self, count: usize) {
        self.ctx.show(self, count).unwrap();
    }
}
