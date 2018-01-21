#![feature(box_patterns)]

use std::fs::File;
use std::fmt::Debug;

extern crate query_planner;
use query_planner::rel::*;
use query_planner::exec::*;


fn main() {

    // define schema for data source (csv file)
    let tt = TupleType {
        columns: vec![
            ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    // open csv file
    let file = File::open("test/people.csv").unwrap();
    let mut csv = CsvRelation::open(&file, &tt).unwrap();

    // create simple filter expression for "id = 2"
    let filter_expr = Rex::BinaryExpr {
        left: Box::new(Rex::TupleValue(0)),
        op: Operator::Eq,
        right: Box::new(Rex::Literal(Value::UnsignedLong(2)))
    };

    // get iterator over data
    let mut it = csv.scan();

    // filter out rows matching the predicate
    while let Some(Ok(t)) = it.next() {
        match evaluate(&t, &tt, &filter_expr) {
            Ok(Value::Boolean(true)) => println!("{:?}", t),
            _ => {}
        }
    }

}
