#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate datafusion;
use datafusion::rel::*;
use datafusion::exec::*;

extern crate serde_json;

fn criterion_benchmark(c: &mut Criterion) {

    c.bench_function("filter_primitive", move |b| {

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
            ColumnData::String((0 .. n).map(|_| "city_name".to_string()).collect()),
            ColumnData::Double((0 .. n).map(|_| 50.0).collect()),
            ColumnData::Double((0 .. n).map(|_| 0.0).collect())
        ]});

        //let lat = df1.col("lat").unwrap();
        let filter_expr = Expr::BinaryExpr {
            left: Box::new(Expr::Column(1)),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(Value::Double(52.0)))
        };

        let ctx = ExecutionContext::local("test/data".to_string());
        let compiled_filter_expr = &compile_expr(&ctx, &filter_expr).unwrap();

        let batch_ref: &Box<Batch> = &batch;

        let col_count = batch.col_count();

        b.iter(move || {

            // evaluate the filter expression against every row
            let filter_eval: ColumnData = (compiled_filter_expr)(batch_ref.as_ref());

            // filter the columns
            let filtered_columns: Vec<ColumnData> = (0..col_count)
                .map(|column_index| { batch_ref.column(column_index).filter(&filter_eval) })
                .collect();

            // create the new batch with the filtered columns
            let filtered_batch: Box<Batch> = Box::new(ColumnBatch { columns: filtered_columns });
        })
    });

}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
