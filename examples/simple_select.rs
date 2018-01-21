#![feature(box_patterns)]

use std::fs::File;
use std::fmt::Debug;

extern crate query_planner;
use query_planner::rel::*;
use query_planner::exec::*;


fn main() {

    // define schema for data source (csv file)
    let schema = TupleType {
        columns: vec![
            ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    let csv_file = Rel::CsvFile { filename: "test/people.csv".to_string(), schema };

    // create simple filter expression for "id = 2"
    let filter_expr = Rex::BinaryExpr {
        left: Box::new(Rex::TupleValue(0)),
        op: Operator::Eq,
        right: Box::new(Rex::Literal(Value::UnsignedLong(2)))
    };

    // create the selection part of the relational plan, referencing the filter expression
    let selection = Rel::Selection { expr: filter_expr, input: Box::new(csv_file) };

    //TODO: create an execution plan
//    let execution_plan = execute(&selection).unwrap();
//
//    // get iterator over data
//    {
//        let it = execution_plan.scan();
//
//        it.for_each(|t| {
//            println!("{:?}", t);
//        });
//    }

//    for &tuple in &it.as_ref() {
//        println!("{:?}", tuple);
//    }

}
