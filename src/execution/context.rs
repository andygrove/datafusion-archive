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

use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::Arc;

use arrow::datatypes::{Schema, Field, DataType};

use super::datasource::DataSource;
use super::super::dfparser::{DFASTNode, DFParser};
use super::error::Result;
use super::super::logicalplan::{FunctionMeta, LogicalPlan};
use super::relation::Relation;
use super::super::sqlplanner::{SqlToRel, SchemaProvider};

pub struct ExecutionContext {
    datasources: Rc<RefCell<HashMap<String, Rc<RefCell<DataSource>>>>>
}

impl ExecutionContext {

    pub fn new() -> Self {
        Self { datasources: Rc::new(RefCell::new(HashMap::new())) }
    }

    pub fn sql(&mut self, sql: &str) -> Result<Rc<Relation>> {

        let ast = DFParser::parse_sql(String::from(sql))?;

        match ast {
//            DFASTNode::CreateExternalTable {
//                name,
//                columns,
//                file_type,
//                header_row,
//                location,
//            } => {
//                let fields: Vec<Field> = columns
//                    .iter()
//                    .map(|c| Field::new(&c.name, convert_data_type(&c.data_type), c.allow_null))
//                    .collect();
//                let schema = Schema::new(fields);
//
//                let df = match file_type {
//                    FileType::CSV => self.load_csv(&location, &schema, header_row, None)?,
//                    FileType::NdJson => self.load_ndjson(&location, &schema, None)?,
//                    FileType::Parquet => self.load_parquet(&location, None)?,
//                };
//
//                self.register(&name, df);
//
//                //TODO: not sure what to return here
//                Ok(Rc::new(DF::new(
//                    self.clone(),
//                    Rc::new(LogicalPlan::EmptyRelation {
//                        schema: Rc::new(Schema::empty()),
//                    }),
//                )))
//            }
            DFASTNode::ANSI(ansi) => {

                let schema_provider: Rc<SchemaProvider> = Rc::new(ExecutionContextSchemaProvider {
                    datasources: self.datasources.clone()
                });

                // create a query planner
                let query_planner = SqlToRel::new(schema_provider);

                // plan the query (create a logical relational plan)
                let plan = query_planner.sql_to_rel(&ansi)?;
                //println!("Logical plan: {:?}", plan);

                let optimized_plan = plan; //push_down_projection(&plan, &HashSet::new());
                //println!("Optimized logical plan: {:?}", new_plan);

                // return the DataFrame
                //Ok(Rc::new(DF::new(self.clone(), new_plan)))

                self.execute(optimized_plan)
            },
            _ => unimplemented!()
        }
    }

    pub fn register_datasource(&mut self, name: &str, ds: Rc<RefCell<DataSource>>) {
        self.datasources.borrow_mut().insert(name.to_string(), ds);
    }

    fn create_schema_provider() {

    }

    fn execute(&mut self, plan: Rc<LogicalPlan>) -> Result<Rc<Relation>> {
        match plan.as_ref() {
            LogicalPlan::Projection { .. } => {
                unimplemented!()
            },
            _ => unimplemented!()
        }
    }

}

struct ExecutionContextSchemaProvider {
    datasources: Rc<RefCell<HashMap<String, Rc<RefCell<DataSource>>>>>

}
impl SchemaProvider for ExecutionContextSchemaProvider {

    fn get_table_meta(&self, name: &str) -> Option<Arc<Schema>> {
        match self.datasources.borrow().get(name) {
            Some(ds) => Some(ds.borrow().schema().clone()),
            None => None
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<FunctionMeta>> {
        unimplemented!()
    }

}