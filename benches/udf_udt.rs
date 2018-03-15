use std::rc::Rc;

#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate datafusion;
use datafusion::data::*;
use datafusion::exec::*;
use datafusion::rel::*;

extern crate serde_json;

fn criterion_benchmark(c: &mut Criterion) {

    c.bench_function("udf_udt", move |b| {

        // create execution context
        let ctx = ExecutionContext::local("test/data".to_string());

        // define schema for data source (csv file)
        let schema = Schema::new(vec![
            Field::new("city", DataType::String, false),
            Field::new("lat", DataType::Double, false),
            Field::new("lng", DataType::Double, false)]);

        // generate some random data
        let n = 1000;
        let batch : Box<Batch> = Box::new(ColumnBatch { columns: vec![
            Rc::new(ColumnData::String((0 .. n).map(|_| "city_name".to_string()).collect())),
            Rc::new(ColumnData::Double((0 .. n).map(|_| 50.0).collect())),
            Rc::new(ColumnData::Double((0 .. n).map(|_| 0.0).collect()))
        ]});

        // ST_Point(lat, lng)
        let expr = Expr::ScalarFunction {
            name: "ST_Point".to_string(),
            args: vec![Expr::Column(1), Expr::Column(2)]
        };

        let ctx = ExecutionContext::local("test/data".to_string());
        let compiled_expr = &compile_expr(&ctx, &expr).unwrap();

        let batch_ref: &Box<Batch> = &batch;

        b.iter(move || {
            // evaluate the scalar function against against every row
            let points: Rc<ColumnData> = (compiled_expr)(batch_ref.as_ref());
        })
    });

}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
