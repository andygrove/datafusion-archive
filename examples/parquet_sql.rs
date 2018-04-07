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
use datafusion::exec::*;

/// This example shows the steps to parse, plan, and execute simple SQL in the current process
fn main() {
    // create execution context
    let mut ctx = ExecutionContext::local("./test/data".to_string());

    let df = ctx.load_parquet("test/data/alltypes_plain.parquet").unwrap();

    ctx.register("all_types_plain", df);

    // define the SQL statement
    let sql = "SELECT int_col FROM all_types_plain";

    // create a data frame
    let df = ctx.sql(&sql).unwrap();

    df.show(10);
}
