#![feature(box_patterns)]

extern crate query_planner;
use query_planner::rel::*;
use query_planner::schema::*;
use query_planner::csvrelation::*;
use query_planner::parser::*;
use query_planner::sqltorel::*;

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

    // parse sql - this needs to be made much more concise
    let sql = String::from("SELECT id, name FROM people");
    let mut tokenizer = Tokenizer { query: sql };
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(tokens);
    let ast = parser.parse().unwrap();

    // create a logical plan
    let plan = SqlToRel::new().sql_to_rel(&ast, &tt);
    println!("Plan: {:?}", plan);

}
