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

use arrow::array::{BinaryArray, Float64Array};
use arrow::csv;
use arrow::datatypes::{DataType, Field, Schema};

use datafusion::execution::context::ExecutionContext;
use datafusion::execution::datasource::CsvDataSource;

//use datafusion::functions::geospatial::st_astext::*;
//use datafusion::functions::geospatial::st_point::*;

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

    ctx.register_datasource("cities", Rc::new(RefCell::new(csv_datasource)));

    // SQL statement doesn't make much sense but demonstrates some features such as math operations,
    // comparisons and type coercion
    let sql = "SELECT MAX(lat) FROM cities WHERE lat > 51.0 AND lat < 53";
    //    let sql = "SELECT city, lat, lng, lat + lng FROM cities WHERE lat > 51.0 AND lat < 53";

    // create a data frame
    let results = ctx.sql(&sql).unwrap();
    let mut ref_mut = results.borrow_mut();

    match ref_mut.next().unwrap() {
        Some(batch) => {
            println!(
                "First batch has {} rows and {} columns",
                batch.num_rows(),
                batch.num_columns()
            );

            //            let city = batch
            //                .column(0)
            //                .as_any()
            //                .downcast_ref::<BinaryArray>()
            //                .unwrap();
            let lat = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            //            let lng = batch
            //                .column(2)
            //                .as_any()
            //                .downcast_ref::<Float64Array>()
            //                .unwrap();
            //            let combined = batch
            //                .column(3)
            //                .as_any()
            //                .downcast_ref::<Float64Array>()
            //                .unwrap();

            for i in 0..batch.num_rows() {
                //                let city_name: String = String::from_utf8(city.get_value(i).to_vec()).unwrap();

                println!("Min Latitude: {}", lat.value(i),);
            }
        }
        _ => println!("No results"),
    }
}
