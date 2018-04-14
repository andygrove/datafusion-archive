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

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use std::cell::RefCell;
use std::fs::File;
use std::rc::Rc;

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::*;
use datafusion::datasources::common::*;
use datafusion::datasources::csv::*;
use datafusion::exec::*;

fn read_csv() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);
    let file = File::open("/mnt/ssd/csv/locations_10000.csv").unwrap();
    let csv = CsvFile::open(file, Rc::new(schema)).unwrap();
    let it = DataSourceIterator::new(Rc::new(RefCell::new(csv)));
    it.for_each(|record_batch| match record_batch {
        Ok(b) => /*println!("new batch with {} rows", b.num_rows())*/ {},
        _ => println!("error"),
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("read_csv", |b| b.iter(|| read_csv()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
