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

extern crate datafusion;
use datafusion::rel::*;
use datafusion::exec::*;
use datafusion::parser::*;
use datafusion::sqltorel::*;

extern crate serde_json;

/// This example shows the steps to parse, plan, and execute simple SQL in the current process
fn main() {

    // define schema for data source (csv file)
    let schema = Schema::new(vec![
        Field::new("id", DataType::UnsignedLong, false),
        Field::new("name", DataType::String, false)
    ]);

    // create a schema registry
    let mut schemas : HashMap<String, Schema> = HashMap::new();
    schemas.insert("people".to_string(), schema.clone());

    // define the SQL statement
    let sql = "SELECT name, id FROM people WHERE id > 4";

    // parse SQL into AST
    let ast = Parser::parse_sql(String::from(sql)).unwrap();

    // create a query planner
    let datafusion = SqlToRel::new(schemas.clone());

    // plan the query (create a logical relational plan)
    let plan = datafusion.sql_to_rel(&ast).unwrap();

    // show the query plan (the json can also be sent to a worker node easily)
    let rel_str = serde_json::to_string_pretty(&plan).unwrap();
    println!("Relational plan: {}", rel_str);

    // create execution context
    let ctx = ExecutionContext::new(schemas.clone());

    // create execution plan
    let execution_plan = ctx.create_execution_plan(&plan).unwrap();

    // execute the query
    let it = execution_plan.scan(&ctx);
    it.for_each(|t| {
        match t {
            Ok(tuple) => println!("Tuple: {:?}", tuple),
            _ => println!("Error")
        }
    });


}
