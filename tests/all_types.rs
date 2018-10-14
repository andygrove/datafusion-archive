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

//#[test]
//fn csv_test_uint8() {
//    csv_project_filter_test("c_uint8", "c_uint8 > CAST(0 AS smallint)", "cast");
//}
//
//#[test]
//fn csv_test_uint16() {
//    csv_project_filter_test("c_uint16", "c_uint16 > CAST(0 AS smallint)", "cast");
//}
//
//#[test]
//fn csv_test_uint32() {
//    csv_project_filter_test("c_uint32", "c_uint32 > CAST(0 AS int)", "cast");
//}
//
//#[test]
//fn csv_test_uint64() {
//    csv_project_filter_test("c_uint64", "c_uint64 > CAST(0 AS bigint)", "cast");
//}

#[test]
fn csv_test_int8() {
    csv_project_filter_test("c_int8", "c_int8 > 0", "positive");
    csv_project_filter_test("c_int8", "c_int8 < 0", "negative");
    csv_project_filter_test("c_int8", "c_int8 < CAST(0 AS smallint)", "cast");
}

#[test]
fn csv_test_int16() {
    csv_project_filter_test("c_int16", "c_int16 > 0", "positive");
    csv_project_filter_test("c_int16", "c_int16 < 0", "negative");
    csv_project_filter_test("c_int16", "c_int16 < CAST(0 as smallint)", "cast");
}

#[test]
fn csv_test_int32() {
    csv_project_filter_test("c_int32", "c_int32 > 0", "positive");
    csv_project_filter_test("c_int32", "c_int32 < 0", "negative");
    csv_project_filter_test("c_int32", "c_int32 < CAST(0 as int)", "cast");
}

#[test]
fn csv_test_int64() {
    csv_project_filter_test("c_int64", "c_int64 > 0", "positive");
    csv_project_filter_test("c_int64", "c_int64 < 0", "negative");
    csv_project_filter_test("c_int64", "c_int64 < CAST(0 as bigint)", "cast");
}

#[test]
fn csv_test_float32() {
    csv_project_filter_test("c_float32", "c_float32 > 0.5", "high");
    csv_project_filter_test("c_float32", "c_float32 < 0.5", "low");
    csv_project_filter_test("c_float32", "c_float32 < CAST(0.5 AS float)", "cast");
}

#[test]
fn csv_test_float32_uint32_comparison() {
    csv_project_filter_test("c_float32", "c_float32 >= 0 ", "high_uint32");
    csv_project_filter_test("c_float32", "c_float32 <= 1", "low_uint32");
    csv_project_filter_test(
        "c_float32",
        "c_float32 <= CAST(1 AS float)",
        "cast_uint32",
    );
}

#[test]
fn csv_test_float64() {
    csv_project_filter_test("c_float64", "c_float64 > 0.5", "high");
    csv_project_filter_test("c_float64", "c_float64 < 0.5", "low");
    csv_project_filter_test("c_float64", "c_float64 < CAST(0.5 as double)", "cast");
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
fn test_scalar_operators() {
    let mut ctx = ExecutionContext::local();
    let schema = numerics_schema(DataType::Int32, DataType::Float32);
    let data = ctx
        .load_csv("test/data/numerics.csv", &schema, true, None)
        .unwrap();
    ctx.register("c", data);

    let sql = "SELECT 2 + 3 FROM c";
    let df = ctx.sql(&sql).unwrap();
    let results = ctx.write_string(df).unwrap();
    assert_eq!("5\n5\n5\n", results);
}

#[test]
fn test_basic_operators_f32() {
    let mut ctx = ExecutionContext::local();
    let schema = numerics_schema(DataType::Int32, DataType::Float32);

    let plus_expr = operation_expr("+", "2", "2.5");
    let minus_expr = operation_expr("-", "2", "2.5");
    let multiply_expr = operation_expr("*", "2", "2.5");
    let divide_expr = operation_expr("/", "2", "2.5");
    let modulo_expr = operation_expr("%", "2", "2.5");

    basic_operation_test(
        &mut ctx,
        &schema,
        &plus_expr,
        "test/data/numerics.csv",
        "numeric_results_plus.csv",
        "numerics_plus.csv",
    );
    basic_operation_test(
        &mut ctx,
        &schema,
        &minus_expr,
        "test/data/numerics.csv",
        "numeric_results_minus.csv",
        "numerics_minus.csv",
    );
    basic_operation_test(
        &mut ctx,
        &schema,
        &multiply_expr,
        "test/data/numerics.csv",
        "numeric_results_multiply.csv",
        "numerics_multiply.csv",
    );
    basic_operation_test(
        &mut ctx,
        &schema,
        &divide_expr,
        "test/data/numerics.csv",
        "numeric_results_divide.csv",
        "numerics_divide.csv",
    );
    basic_operation_test(
        &mut ctx,
        &schema,
        &modulo_expr,
        "test/data/numerics.csv",
        "numeric_results_modulo.csv",
        "numerics_modulo.csv",
    );
}

#[test]
fn test_basic_operators_f64() {
    let mut ctx = ExecutionContext::local();
    let schema = numerics_schema(DataType::Int64, DataType::Float64);

    let plus_expr = operation_expr("+", "2", "2.5");
    let minus_expr = operation_expr("-", "2", "2.5");
    let multiply_expr = operation_expr("*", "2", "2.5");
    let divide_expr = operation_expr("/", "2", "2.5");
    let modulo_expr = operation_expr("%", "2", "2.5");

    basic_operation_test(
        &mut ctx,
        &schema,
        &plus_expr,
        "test/data/numerics.csv",
        "numeric_results_plus_f64.csv",
        "numerics_plus_f64.csv",
    );
    basic_operation_test(
        &mut ctx,
        &schema,
        &minus_expr,
        "test/data/numerics.csv",
        "numeric_results_minus_f64.csv",
        "numerics_minus_f64.csv",
    );
    basic_operation_test(
        &mut ctx,
        &schema,
        &multiply_expr,
        "test/data/numerics.csv",
        "numeric_results_multiply_f64.csv",
        "numerics_multiply_f64.csv",
    );
    basic_operation_test(
        &mut ctx,
        &schema,
        &divide_expr,
        "test/data/numerics.csv",
        "numeric_results_divide_f64.csv",
        "numerics_divide_f64.csv",
    );
    basic_operation_test(
        &mut ctx,
        &schema,
        &modulo_expr,
        "test/data/numerics.csv",
        "numeric_results_modulo_f64.csv",
        "numerics_modulo_f64.csv",
    );
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

fn operation_expr(op: &str, scalar: &str, scalar_f: &str) -> String {
    format!(
        "a {} b, {} {} a, {} {} a, a_f {} b_f, {} {} a_f, {} {} a_f",
        op, scalar, op, scalar_f, op, op, scalar, op, scalar_f, op
    )
}

fn basic_operation_test(
    ctx: &mut ExecutionContext,
    schema: &Schema,
    expr: &str,
    filename: &str,
    output: &str,
    exp: &str,
) {
    let data = ctx.load_csv(filename, schema, true, None).unwrap();
    ctx.register("c", data);

    let sql = format!("SELECT {} FROM c", expr);
    let output_filename = format!("target/{}", output);
    let expected_filename = format!("test/data/expected/{}", exp);

    println!("sql = {}", sql);
    let df = ctx.sql(&sql).unwrap();
    ctx.write_csv(df, &output_filename).unwrap();

    let expected = read_file(&expected_filename);
    let realized = read_file(&output_filename);

    assert_eq!(expected, realized);
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

fn numerics_schema(dt1: DataType, dt2: DataType) -> Schema {
    Schema::new(vec![
        Field::new("a", dt1.clone(), false),
        Field::new("b", dt1.clone(), false),
        Field::new("a_f", dt2.clone(), false),
        Field::new("b_f", dt2.clone(), false),
    ])
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
