#![feature(box_patterns)]

extern crate query_planner;
use query_planner::rel::*;
use query_planner::exec::*;
use query_planner::parser::*;
use query_planner::sqltorel::*;

extern crate serde_json;


fn main() {

    // parse sql - this needs to be made much more concise
    let ast = Parser::parse_sql(String::from("SELECT id, name FROM people WHERE id > 4")).unwrap();

    // define schema for data source (csv file)
    let tt = TupleType {
        columns: vec![
            ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    // create execution plan
    let mut ctx = ExecutionContext::new();
    ctx.register_table("people".to_string(), tt.clone());


    // create a logical plan
    let plan = SqlToRel::new(ctx.schemas.clone())
        .sql_to_rel(&ast, &tt).unwrap();


    let rel_str = serde_json::to_string_pretty(&plan).unwrap();
    println!("Relational plan: {}", rel_str);

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
