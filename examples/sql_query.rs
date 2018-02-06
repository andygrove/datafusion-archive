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

extern crate datafusion;
use datafusion::rel::*;
use datafusion::exec::*;

extern crate serde_json;

/// This example shows the steps to parse, plan, and execute simple SQL in the current process
fn main() {

    // create execution context
    let mut ctx = ExecutionContext::new();

    // define an external table
    ctx.sql("CREATE EXTERNAL TABLE uk_cities (\
        city VARCHAR(100), \
        lat DOUBLE, \
        lng DOUBLE)").unwrap();

//    // define schema for data source (csv file)
//    let schema = Schema::new(vec![
//        Field::new("city", DataType::String, false),
//        Field::new("lat", DataType::Double, false),
//        Field::new("lng", DataType::Double, false)]);
//
//    // register the csv file as a table that can be queried via sql
//    ctx.define_schema("uk_cities", &schema);

    // define the SQL statement
    let sql = "SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities"; // WHERE lat < 53

    let df1 = ctx.sql(&sql).unwrap();
    println!("df1: {}", df1.schema().to_string());

    // write the results to a file
    df1.write("_southern_cities.csv").unwrap();


}
