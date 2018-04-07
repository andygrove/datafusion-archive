// Copyright 2018 Grove Enterprises LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::rc::Rc;

#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::*;
use datafusion::exec::*;
use datafusion::rel::*;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("udf_udt", move |b| {
        // create execution context
        let ctx = ExecutionContext::local("test/data".to_string());

        // define schema for data source (csv file)
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        //TODO: fix this when finished moving to Arrow format

        //        // generate some random data
        //        let n = 1000;
        //        let n = 1000;
        //        let batch : Box<Batch> = Box::new(ColumnBatch { row_count: n, columns: vec![
        //            Rc::new(Value::Column(
        //                Rc::new(Field::new("city_name", DataType::Utf8, false)),
        //                Rc::new(Array::new(ArrayData::Utf8((0 .. n).map(|_| "city_name".to_string()).collect())))
        //            )),
        //            Rc::new(Value::Column(
        //                Rc::new(Field::new("city_name", DataType::Utf8, false)),
        //                Rc::new(Array::new(ArrayData::Float64((0 .. n).map(|_| 50.0).collect())))
        //            )),
        //            Rc::new(Value::Column(
        //                Rc::new(Field::new("city_name", DataType::Utf8, false)),
        //                Rc::new(Array::new(ArrayData::Float64((0 .. n).map(|_| 0.0).collect())))
        //            ))
        //        ]});
        //
        //
        //        // ST_Point(lat, lng)
        //        let expr = Expr::ScalarFunction {
        //            name: "ST_Point".to_string(),
        //            args: vec![Expr::Column(1), Expr::Column(2)]
        //        };
        //
        //        let ctx = ExecutionContext::local("test/data".to_string());
        //        let compiled_expr = &compile_expr(&ctx, &expr).unwrap();
        //
        //        let batch_ref: &Box<Batch> = &batch;
        //
        //        b.iter(move || {
        //            // evaluate the scalar function against against every row
        //            let points: Rc<Value> = (compiled_expr)(batch_ref.as_ref());
        //        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
