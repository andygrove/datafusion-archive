#![feature(box_patterns)]

extern crate query_planner;
use query_planner::rel::*;
use query_planner::exec::*;

extern crate serde_json;

fn main() {

    // define schema for data source (csv file)
    let schema = TupleType {
        columns: vec![
            ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    let csv_file = Rel::CsvFile {
        filename: "test/people.csv".to_string(),
        schema: schema.clone()
    };

    // create simple filter expression for "id = 2"
    let filter_expr = Rex::BinaryExpr {
        left: Box::new(Rex::TupleValue(0)),
        op: Operator::Eq,
        right: Box::new(Rex::Literal(Value::UnsignedLong(2)))
    };

    // create the selection part of the relational plan, referencing the filter expression
    let plan = Rel::Selection {
        expr: filter_expr,
        input: Box::new(csv_file),
        schema: schema.clone()
    };

    let rel_str = serde_json::to_string_pretty(&plan).unwrap();

    println!("Relational plan: {}", rel_str);

    // create execution plan
    let execution_plan = create_execution_plan(&plan).unwrap();

    // execute the query
    let it = execution_plan.scan();
    it.for_each(|t| {
        match t {
            Ok(tuple) => println!("Tuple: {:?}", tuple),
            _ => println!("Error")
        }
    });
}
