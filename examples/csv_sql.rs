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

use std::cell::RefCell;
use std::fs::File;
use std::rc::Rc;
use std::sync::Arc;

extern crate arrow;
extern crate datafusion;

use arrow::csv;
use arrow::datatypes::*;

use datafusion::execution::datasource::{CsvDataSource, DataSource};
use datafusion::execution::context::ExecutionContext;

//use datafusion::functions::geospatial::st_astext::*;
/// This example shows the steps to parse, plan, and execute simple SQL in the current process
fn main() {
    // create execution context
    let mut ctx = ExecutionContext::new();
//    ctx.register_scalar_function(Rc::new(STPointFunc {}));
//    ctx.register_scalar_function(Rc::new(STAsText {}));

    // define schema for data source (csv file)
    let schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]));

    let file = File::open("test/data/uk_cities.csv").unwrap();

    let arrow_csv_reader = csv::Reader::new(file, schema.clone(), true, 1024, None);

    let csv_datasource = CsvDataSource::new(schema.clone(), arrow_csv_reader);

    ctx.register_datasource("cities", Rc::new(RefCell::new(csv_datasource)) );

    // define the SQL statement
    let sql = "SELECT lat, lng FROM cities";
//    let sql = "SELECT ST_AsText(ST_Point(lat, lng)) FROM cities WHERE lat < 53.0";

    // create a data frame
    let results = ctx.sql(&sql).unwrap();
//
//    // show the first 10 rows of output
//    df1.show(10)
}

//use datafusion::functions::geospatial::st_point::*;
