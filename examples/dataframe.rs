#![feature(box_patterns)]

use std::collections::HashMap;

extern crate query_planner;
use query_planner::rel::*;
use query_planner::exec::*;
use query_planner::dataframe::*;

extern crate serde_json;

/// This example shows the use of the DataFrame API to define a query plan
fn main() {

    // define schema for data source (csv file)
    let schema = TupleType {
        columns: vec![
            ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    // create a schema registry
    let mut schemas : HashMap<String, TupleType> = HashMap::new();
    schemas.insert("people".to_string(), schema.clone());

    // create execution context
    let ctx = ExecutionContext::new(schemas.clone());

    // open a CSV file as a dataframe
    let df = ctx.load("test/people.csv").unwrap();

    // perform some transformations
    let df2 = df.filter(df.col("id").unwrap().eq(Value::UnsignedLong(4))).unwrap();

    // write the results to a file
    df2.write("person4.csv");

}
