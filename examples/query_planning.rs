#![feature(box_patterns)]

extern crate query_planner;
use query_planner::rel::*;
use query_planner::schema::*;
use query_planner::csvrelation::*;
use query_planner::parser::*;
use query_planner::sqltorel::*;

fn main() {

    // parse sql - this needs to be made much more concise
    let ast = Parser::parse_sql(String::from("SELECT id, name FROM people")).unwrap();

    // define schema for data source (csv file)
    let tt = TupleType {
        columns: vec![
            ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    // create a logical plan
    let plan = SqlToRel::new().sql_to_rel(&ast, &tt);
    println!("Plan: {:?}", plan);

}
