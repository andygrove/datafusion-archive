#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate datafusion;
use datafusion::rel::*;
use datafusion::exec::*;

extern crate serde_json;

fn sql() {

    // create execution context
    let mut ctx = ExecutionContext::new();

    // define schema for data source (csv file)
    let schema = Schema::new(vec![
        Field::new("city", DataType::String, false),
        Field::new("lat", DataType::Double, false),
        Field::new("lng", DataType::Double, false)]);

    // register the csv file as a table that can be queried via sql
    ctx.define_schema("uk_cities", &schema);

    // define the SQL statement
    let sql = "SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities"; // WHERE lat < 53

    let df1 = ctx.sql(&sql).unwrap();

    // write the results to a file
    df1.write("_southern_cities.csv").unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("sql_query", |b| b.iter(|| sql()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
