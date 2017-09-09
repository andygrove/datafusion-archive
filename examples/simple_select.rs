#![feature(box_patterns)]

extern crate query_planner;
use query_planner::rel::*;
use query_planner::schema::*;
use query_planner::csvrelation::*;
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
    let mut csv = CsvRelation::open(String::from("people.csv"), tt.clone());

    // create simple filter expression for "id = 2"
    let filter_expr = Rex::BinaryExpr {
        left: Box::new(Rex::TupleValue(0)),
        op: Operator::Eq,
        right: Box::new(Rex::Literal(Value::UnsignedLong(2)))
    };

    // get iterator over data
    let mut it = csv.scan();

    // filter out rows matching the predicate
    while let Some(t) = it.next() {
        match evaluate(&t, &tt, &filter_expr) {
            Ok(Value::Boolean(true)) => println!("{:?}", t),
            _ => {}
        }
    }

}
