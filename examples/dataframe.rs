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

use std::collections::HashMap;

extern crate datafusion;
use datafusion::rel::*;
use datafusion::exec::*;
//use datafusion::dataframe::*;

extern crate serde_json;

/// This example shows the use of the DataFrame API to define a query plan
fn main() {

    // define schema for data source (csv file)
    let schema = TupleType::new(vec![
        Field::new("id", DataType::UnsignedLong, false),
        Field::new("name", DataType::String, false)
        ]);

    // create a schema registry
    let mut schemas : HashMap<String, TupleType> = HashMap::new();
    schemas.insert("people".to_string(), schema.clone());

    // create execution context
    let ctx = ExecutionContext::new(schemas.clone());

    // open a CSV file as a dataframe
    let df = ctx.load("test/data/people.csv", &schema).unwrap();

    // filter on id
    let id = df.col("id").unwrap();
    let id_value = Rex::Literal(Value::UnsignedLong(4));
    let df2 = df.filter(id.eq(&id_value)).unwrap();

    // write the results to a file
    df2.write("person4.csv").unwrap();

}
