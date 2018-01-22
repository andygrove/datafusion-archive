#![feature(box_patterns)]

use std::collections::HashMap;

extern crate query_planner;
use query_planner::rel::*;
use query_planner::exec::*;
use query_planner::parser::*;
use query_planner::sqltorel::*;

extern crate serde_json;

/// This example shows the steps to parse, plan, and execute simple SQL in the current process
fn main() {

    let sql = "SELECT name, id FROM people WHERE id > 4";

    // parse SQL into AST
    let ast = Parser::parse_sql(String::from(sql)).unwrap();

    // define schema for a csv file
    let schema = TupleType {
        columns: vec![
            ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    // create a schema registry
    let mut schemas : HashMap<String, TupleType> = HashMap::new();
    schemas.insert("people".to_string(), schema.clone());

    // create a query planner
    let query_planner = SqlToRel::new(schemas.clone());

    // plan the query (create a logical relational plan)
    let plan = query_planner.sql_to_rel(&ast).unwrap();

    // show the query plan (the json can also be sent to a worker node easily)
    let rel_str = serde_json::to_string_pretty(&plan).unwrap();
    println!("Relational plan: {}", rel_str);

    // create execution plan
    let ctx = ExecutionContext::new(schemas.clone());
    let execution_plan = ctx.create_execution_plan(&plan).unwrap();

    // execute the query
    let it = execution_plan.scan();
    it.for_each(|t| {
        match t {
            Ok(tuple) => println!("Tuple: {:?}", tuple),
            _ => println!("Error")
        }
    });


}
