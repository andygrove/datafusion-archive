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

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::*;
use datafusion::exec::*;
use datafusion::functions::geospatial::*;

/// This example shows the steps to parse, plan, and execute simple SQL in the current process
fn main() {
    // create execution context
    let mut ctx = ExecutionContext::local();
    ctx.register_function(Rc::new(STPointFunc {}));
    ctx.register_function(Rc::new(STAsText {}));

    // define schema for data source (csv file)
    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

    // open a CSV file as a dataframe
    let uk_cities = ctx.load_csv("test/data/uk_cities.csv", &schema).unwrap();
    println!("uk_cities schema: {}", uk_cities.schema().to_string());

    ctx.register("uk_cities", uk_cities);

    // define the SQL statement
    let sql = "SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities WHERE lat < 53.0";

    // create a data frame
    let df1 = ctx.sql(&sql).unwrap();

    // write the results to a file
    ctx.write_csv(df1, "_southern_cities.csv").unwrap();
}
