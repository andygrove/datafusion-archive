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
    df.show(10);
    ctx.write_csv(df, "target/csv_query_all_types.csv").unwrap();

    let expected_result = read_file("test/data/expected/csv_query_all_types.csv");
    assert_eq!(
        expected_result,
        read_file("./target/csv_query_all_types.csv")
    );
}

#[test]
fn csv_test_uint8() {
    csv_project_filter_test("c_uint8", "c_uint8 > CAST(0 AS uint8)", "cast");
}

#[test]
fn csv_test_uint16() {
    csv_project_filter_test("c_uint16", "c_uint16 > CAST(0 AS uint16)", "cast");
}

#[test]
fn csv_test_uint32() {
    csv_project_filter_test("c_uint32", "c_uint32 > CAST(0 AS uint32)", "cast");
}

#[test]
fn csv_test_uint64() {
    csv_project_filter_test("c_uint64", "c_uint64 > CAST(0 AS uint64)", "cast");
}

#[test]
fn csv_test_int8() {
    csv_project_filter_test("c_int8", "c_int8 > 0", "positive");
    csv_project_filter_test("c_int8", "c_int8 < 0", "negative");
    csv_project_filter_test("c_int8", "c_int8 < CAST(0 AS int8)", "cast");
}

#[test]
fn csv_test_int16() {
    csv_project_filter_test("c_int16", "c_int16 > 0", "positive");
    csv_project_filter_test("c_int16", "c_int16 < 0", "negative");
    csv_project_filter_test("c_int16", "c_int16 < CAST(0 as int16)", "cast");
}

#[test]
fn csv_test_int32() {
    csv_project_filter_test("c_int32", "c_int32 > 0", "positive");
    csv_project_filter_test("c_int32", "c_int32 < 0", "negative");
    csv_project_filter_test("c_int32", "c_int32 < CAST(0 as int32)", "cast");
}

#[test]
fn csv_test_int64() {
    csv_project_filter_test("c_int64", "c_int64 > 0", "positive");
    csv_project_filter_test("c_int64", "c_int64 < 0", "negative");
    csv_project_filter_test("c_int64", "c_int64 < CAST(0 as int64)", "cast");
}

#[test]
fn csv_test_float32() {
    csv_project_filter_test("c_float32", "c_float32 > 0.5", "high");
    csv_project_filter_test("c_float32", "c_float32 < 0.5", "low");
    csv_project_filter_test("c_float32", "c_float32 < CAST(0.5 as float32)", "cast");
}

#[test]
fn csv_test_float32_uint32_comparison() {
    csv_project_filter_test("c_float32", "c_float32 >= 0 ", "high_uint32");
    csv_project_filter_test("c_float32", "c_float32 <= 1", "low_uint32");
    csv_project_filter_test("c_float32", "c_float32 <= CAST(1 as float32)", "cast_uint32");
}

#[test]
fn csv_test_float64() {
    csv_project_filter_test("c_float64", "c_float64 > 0.5", "high");
    csv_project_filter_test("c_float64", "c_float64 < 0.5", "low");
    csv_project_filter_test("c_float64", "c_float64 < CAST(0.5 as float64)", "cast");
}

#[test]
fn csv_test_comparison_ops() {
    csv_project_filter_test("c_int8", "c_int8 > 33 AND 33 < c_int8", "gt");
    csv_project_filter_test("c_int8", "c_int8 >= 33 AND 33 <= c_int8", "gteq");
    csv_project_filter_test("c_int8", "c_int8 < 99 AND 99 > c_int8", "lt");
    csv_project_filter_test("c_int8", "c_int8 <= 90 AND 90 >= c_int8", "lteq");
    csv_project_filter_test("c_int8", "c_int8 = 0 AND 0 = c_int8", "eq");
    csv_project_filter_test("c_int8", "c_int8 != 0 AND 0 != c_int8", "noteq");
}

#[test]
fn csv_test_compare_columns() {
    csv_project_filter_test("c_int8", "c_int8 < c_int16", "col_lt");
    csv_project_filter_test("c_int8", "c_int8 <= c_int16", "col_lteq");
    csv_project_filter_test("c_int8", "c_int8 > c_int16", "col_gt");
    csv_project_filter_test("c_int8", "c_int8 >= c_int16", "col_gteq");
    csv_project_filter_test("c_int8", "c_int8 = c_int16", "col_eq");
    csv_project_filter_test("c_int8", "c_int8 != c_int16", "col_noteq");
}

// not implemented yet
//#[test]
//fn scalar_comparisons() {
//    csv_project_filter_test("c_int8", "44 > 33", "scalar_gt");
//    csv_project_filter_test("c_int8", "44 >= 33", "scalar_gteq");
//    csv_project_filter_test("c_int8", "44 < 99", "scalar_lt");
//    csv_project_filter_test("c_int8", "44 <= 90", "scalar_lteq");
//    csv_project_filter_test("c_int8", "44 = 0", "scalar_eq");
//    csv_project_filter_test("c_int8", "44 != 0", "scalar_noteq");
//}

#[test]
fn test_basic_operators() {
    let mut ctx = ExecutionContext::local();
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("a_f", DataType::Float32, false),
        Field::new("b_f", DataType::Float32, false),
    ]);
    let data = ctx.load_csv("test/data/numerics.csv", &schema, true, None)
        .unwrap();
    ctx.register("c", data);

    let sql = "SELECT a + b, \
    a - b, \
    a * b, \
    a / b, \
    a % b, \
    a_f + b_f, \
    a_f - b_f, \
    a_f * b_f, \
    a_f / b_f, \
    a_f % b_f \
    FROM c";

    let df = ctx.sql(&sql).unwrap();
    ctx.write_csv(df, "target/numeric_results.csv").unwrap();

    let expected = read_file("test/data/expected/numerics.csv");
    let realized = read_file("./target/numeric_results.csv");
    println!("{}", realized);
    assert_eq!(expected, realized);
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
    ctx.write_csv(df, "target/parquet_query_all_types.csv")
        .unwrap();

    let expected_result = read_file("test/data/expected/parquet_query_all_types.csv");
    assert_eq!(
        expected_result,
        read_file("./target/parquet_query_all_types.csv")
    );
}

#[test]
fn parquet_aggregate() {
    let mut ctx = ExecutionContext::local();
    load_parquet(&mut ctx, "test/data/all_types_flat.parquet");

    // define the SQL statement
    let sql = "SELECT \
        COUNT(1), COUNT(*), \
        MIN(c_bool), MAX(c_bool), \
        MIN(c_uint8), MAX(c_uint8), \
        MIN(c_uint16), MAX(c_uint16), \
        MIN(c_uint32), MAX(c_uint32), \
        MIN(c_uint64), MAX(c_uint64), \
        MIN(c_int8), MAX(c_int8), \
        MIN(c_int16), MAX(c_int16), \
        MIN(c_int32), MAX(c_int32), \
        MIN(c_int64), MAX(c_int64), \
        MIN(c_float32), MAX(c_float32), \
        MIN(c_float64), MAX(c_float64), \
        MIN(c_utf8), MAX(c_utf8), \
        SUM(c_int8), \
        SUM(c_int16), \
        SUM(c_int32), \
        SUM(c_int64), \
        SUM(c_uint8), \
        SUM(c_uint16), \
        SUM(c_uint32), \
        SUM(c_uint64), \
        SUM(c_float32), \
        SUM(c_float64) \
    FROM all_types";

    // create a data frame
    let df = ctx.sql(&sql).unwrap();
    ctx.write_csv(df, "target/parquet_aggregate_all_types.csv")
        .unwrap();

    let expected_result = read_file("test/data/expected/parquet_aggregate_all_types.csv");
    assert_eq!(
        expected_result,
        read_file("./target/parquet_aggregate_all_types.csv")
    );
}

#[test]
fn csv_aggregate() {
    let mut ctx = ExecutionContext::local();
    load_csv(&mut ctx, "test/data/all_types_flat.csv");

    // define the SQL statement
    let sql = "SELECT \
        COUNT(1), COUNT(*), \
        MIN(c_bool), MAX(c_bool), \
        MIN(c_uint8), MAX(c_uint8), \
        MIN(c_uint16), MAX(c_uint16), \
        MIN(c_uint32), MAX(c_uint32), \
        MIN(c_uint64), MAX(c_uint64), \
        MIN(c_int8), MAX(c_int8), \
        MIN(c_int16), MAX(c_int16), \
        MIN(c_int32), MAX(c_int32), \
        MIN(c_int64), MAX(c_int64), \
        MIN(c_float32), MAX(c_float32), \
        MIN(c_float64), MAX(c_float64), \
        MIN(c_utf8), MAX(c_utf8)
    FROM all_types";

    // create a data frame
    let df = ctx.sql(&sql).unwrap();
    ctx.write_csv(df, "target/csv_aggregate_all_types.csv")
        .unwrap();

    let expected_result = read_file("test/data/expected/csv_aggregate_all_types.csv");
    assert_eq!(
        expected_result,
        read_file("./target/csv_aggregate_all_types.csv")
    );
}

#[test]
fn csv_aggregate_group_by_bool() {
    let mut ctx = ExecutionContext::local();
    load_csv(&mut ctx, "test/data/all_types_flat.csv");

    // define the SQL statement
    let sql = "SELECT \
        c_bool, \
        MIN(c_uint8), MAX(c_uint8), \
        MIN(c_uint16), MAX(c_uint16), \
        MIN(c_uint32), MAX(c_uint32), \
        MIN(c_uint64), MAX(c_uint64), \
        MIN(c_int8), MAX(c_int8), \
        MIN(c_int16), MAX(c_int16), \
        MIN(c_int32), MAX(c_int32), \
        MIN(c_int64), MAX(c_int64), \
        MIN(c_float32), MAX(c_float32), \
        MIN(c_float64), MAX(c_float64), \
        MIN(c_utf8), MAX(c_utf8)
    FROM all_types GROUP BY c_bool";

    // create a data frame
    let df = ctx.sql(&sql).unwrap();
    ctx.write_csv(df, "target/csv_aggregate_by_c_bool.csv")
        .unwrap();

    let expected_result = read_file("test/data/expected/csv_aggregate_by_c_bool.csv");
    assert_eq!(
        expected_result,
        read_file("./target/csv_aggregate_by_c_bool.csv")
    );
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
