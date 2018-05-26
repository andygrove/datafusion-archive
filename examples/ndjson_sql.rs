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

extern crate arrow;
extern crate datafusion;

use arrow::datatypes::*;
use datafusion::exec::*;

fn main() {
    // create execution context
    let mut ctx = ExecutionContext::local();

    let schema = Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Float64, false),
    ]);

    // open file as a dataframe
    let ndjson = ctx
        .load_ndjson("test/data/example1.ndjson", &schema, None)
        .unwrap();

    ctx.register("ndjson", ndjson);

    // define the SQL statement
    let sql = "SELECT a, b, c FROM ndjson";

    // create a data frame
    let df1 = ctx.sql(&sql).unwrap();

    // show the first 10 rows of output
    df1.show(10)
}
