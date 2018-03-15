#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate datafusion;
use datafusion::data::*;
use datafusion::exec::*;
use datafusion::rel::*;

extern crate serde_json;

fn dataframe() {

    // create execution context
    let ctx = ExecutionContext::local("test/data".to_string());

    // define schema for data source (csv file)
    let schema = Schema::new(vec![
        Field::new("city", DataType::String, false),
        Field::new("lat", DataType::Double, false),
        Field::new("lng", DataType::Double, false)]);

    // open a CSV file as a dataframe
    let df1 = ctx.load("test/data/uk_cities.csv", &schema).unwrap();

    // filter on lat > 52.0
    let lat = df1.col("lat").unwrap();
    let value = Expr::Literal(Value::Double(52.0));
    let df2 = df1.filter(lat.gt(&value)).unwrap();

    // apply a projection using a scalar function to create a complex type
    // invoke custom code as a scalar UDF
    let st_point = ctx.udf("ST_Point",vec![
        df2.col("lat").unwrap(),
        df2.col("lng").unwrap()]);

    let df3 = df2.select(vec![st_point]).unwrap();

    // write the results to a file
    ctx.write(df3, "_northern_cities.csv").unwrap();

}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("dataframe", |b| b.iter(|| dataframe()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
