#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::*;
use datafusion::exec::*;
use datafusion::rel::*;

fn sql() {
    // create execution context
    let mut ctx = ExecutionContext::local("test/data".to_string());

    // define schema for data source (csv file)
    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

    // register the csv file as a table that can be queried via sql
    ctx.define_schema("uk_cities", &schema);

    // define the SQL statement
    let sql = "SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities"; // WHERE lat < 53

    let df1 = ctx.sql(&sql).unwrap();

    // write the results to a file
    ctx.write_csv(df1, "_southern_cities.csv").unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("sql_query", |b| b.iter(|| sql()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
