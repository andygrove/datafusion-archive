extern crate arrow;
extern crate datafusion;

use std::fs::File;
use std::io::prelude::*;

use arrow::datatypes::*;
use datafusion::exec::*;

#[test]
fn csv_query_all_types() {
    let mut ctx = ExecutionContext::local();
    load_csv(&mut ctx, "test/data/all_types_flat.csv");

    // define the SQL statement
    let sql = "SELECT c_bool, \
        c_uint8, c_uint16, c_uint32, c_uint64, \
        c_int8, c_int16, c_int32, c_int64, \
        c_float32, c_float64, \
        c_utf8
    FROM all_types
    WHERE c_float64 > 0.0 AND c_float64 < 0.1";

    // create a data frame
    let df = ctx.sql(&sql).unwrap();
    ctx.write_csv(df, "target/csv_query_all_types.csv").unwrap();

    let expected_result = read_file("test/data/expected/csv_query_all_types.csv");
    assert_eq!(expected_result, read_file("./target/csv_query_all_types.csv"));
}

#[test]
fn csv_test_int8() {
    csv_project_filter_test("c_int8", "c_int8 > 0", "positive");
    csv_project_filter_test("c_int8", "c_int8 < 0", "negative");
}

#[test]
fn csv_test_int16() {
    csv_project_filter_test("c_int16", "c_int16 > 0", "positive");
    csv_project_filter_test("c_int16", "c_int16 < 0", "negative");
}

#[test]
fn csv_test_int32() {
    csv_project_filter_test("c_int32", "c_int32 > 0", "positive");
    csv_project_filter_test("c_int32", "c_int32 < 0", "negative");
}

#[test]
fn csv_test_int64() {
    csv_project_filter_test("c_int64", "c_int64 > 0", "positive");
    csv_project_filter_test("c_int64", "c_int64 < 0", "negative");
}

#[test]
fn csv_test_float32() {
    csv_project_filter_test("c_float32", "c_float32 > 0.5", "high");
    csv_project_filter_test("c_float32", "c_float32 < 0.5", "low");
}

#[test]
fn csv_test_float64() {
    csv_project_filter_test("c_float64", "c_float64 > 0.5", "high");
    csv_project_filter_test("c_float64", "c_float64 < 0.5", "low");
}

#[test]
fn parquet_query_all_types() {
    let mut ctx = ExecutionContext::local();
    load_parquet(&mut ctx, "test/data/all_types_flat.parquet");

    // define the SQL statement
    let sql = "SELECT c_bool, \
        c_uint8, c_uint16, c_uint32, c_uint64, \
        c_int8, c_int16, c_int32, c_int64, \
        c_float32, c_float64, \
        c_utf8
    FROM all_types
    WHERE c_float64 > 0.0 AND c_float64 < 0.1";

    // create a data frame
    let df = ctx.sql(&sql).unwrap();
    ctx.write_csv(df, "target/parquet_query_all_types.csv").unwrap();

    let expected_result = read_file("test/data/expected/parquet_query_all_types.csv");
    assert_eq!(expected_result, read_file("./target/parquet_query_all_types.csv"));
}




fn csv_project_filter_test(col: &str, expr: &str, filename: &str) {
    let output_filename = format!("target/{}_{}.csv", col, filename);
    let expected_filename = format!("test/data/expected/{}_{}.csv", col, filename);
    let mut ctx = ExecutionContext::local();
    load_csv(&mut ctx, "test/data/all_types_flat.csv");
    let sql = format!("SELECT {} FROM all_types WHERE {}", col, expr);
    let df = ctx.sql(&sql).unwrap();
    ctx.write_csv(df, &output_filename).unwrap();
    let expected_result = read_file(&expected_filename);
    assert_eq!(expected_result, read_file(&output_filename));
}

fn read_file(filename: &str) -> String {
    let mut file = File::open(filename).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    contents
}

fn load_csv(ctx: &mut ExecutionContext, filename: &str) {
    let schema = create_schema();
    let csv = ctx.load_csv(filename, &schema, false, None).unwrap();
    ctx.register("all_types", csv);
}

fn load_parquet(ctx: &mut ExecutionContext, filename: &str) {
    let csv = ctx.load_parquet(filename, None).unwrap();
    ctx.register("all_types", csv);
}

fn create_schema() -> Schema {
    Schema::new(vec![
        Field::new("c_bool", DataType::Boolean, false),
        Field::new("c_uint8", DataType::UInt8, false),
        Field::new("c_uint16", DataType::UInt16, false),
        Field::new("c_uint32", DataType::UInt32, false),
        Field::new("c_uint64", DataType::UInt64, false),
        Field::new("c_int8", DataType::Int8, false),
        Field::new("c_int16", DataType::Int16, false),
        Field::new("c_int32", DataType::Int32, false),
        Field::new("c_int64", DataType::Int64, false),
        Field::new("c_float32", DataType::Float32, false),
        Field::new("c_float64", DataType::Float64, false),
        Field::new("c_utf8", DataType::Utf8, false),
    ])
}