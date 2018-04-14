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

use std::fs::File;
use std::rc::Rc;

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::*;
use arrow::array::*;
use datafusion::datasources::common::*;
use datafusion::datasources::quiver::*;
use datafusion::types::Value;

/// This example demonstrates low-level reading and writing Quiver files
fn main() {

    // define schema for data source (csv file)
    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

    let names: Rc<Array> = Rc::new(Array::from(vec!["Elgin, Scotland, the UK".to_string(),
                                      "Stoke-on-Trent, Staffordshire, the UK".to_string()]));

    let lats: Rc<Array> = Rc::new(Array::from(vec![57.653484, 53.002666]));
    let lngs: Rc<Array> = Rc::new(Array::from(vec![-3.335724, -2.179404]));

    // write the quiver file
    {
        let file = File::create("_uk_cities.quiver").unwrap();
        let mut w = QuiverWriter::new(file);
        w.write_schema(&schema).unwrap();
        w.write_row_group(vec![names, lats, lngs]).unwrap();
    }

    // read the quiver file
    let file = File::open("_uk_cities.quiver").unwrap();
    let mut r = QuiverReader::new(file);
    let schema = r.read_schema();
    println!("Schema: {:?}", schema);

    //let data = r.read_row_group();

}
