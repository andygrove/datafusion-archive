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
use datafusion::exec::*;
use datafusion::functions::conversions::*;

fn main() {
    // download data file from https://www.kaggle.com/kaggle/sf-salaries/discussion/18736
    let path = "datasets/Salaries.csv";
    match File::open(path) {
        Ok(_) => {
            // create execution context
            let mut ctx = ExecutionContext::local();
            ctx.register_scalar_function(Rc::new(ToFloat64Function{}));

            // define schema for data source (csv file)
            let schema = Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("employee_name", DataType::Utf8, false),
                Field::new("job_title", DataType::Utf8, false),
                Field::new("base_pay", DataType::Utf8, false),
                Field::new("overtime_pay", DataType::Utf8, false),
                Field::new("other_pay", DataType::Utf8, false),
                Field::new("benefits", DataType::Utf8, false),
                Field::new("total_pay", DataType::Utf8, false),
                Field::new("total_pay_benefits", DataType::Utf8, false),
                Field::new("year", DataType::Utf8, false),
                Field::new("notes", DataType::Utf8, true),
                Field::new("agency", DataType::Utf8, false),
                Field::new("status", DataType::Utf8, false),
            ]);

            // open a CSV file as a dataframe
            let salaries = ctx.load_csv(path, &schema, true).unwrap();

            // register as a table so we can run SQL against it
            ctx.register("salaries", salaries);

            // define the SQL statement
            let sql = "SELECT year, MIN(CAST(base_pay AS FLOAT)), MAX(CAST(base_pay AS FLOAT)) \
                            FROM salaries \
                            WHERE base_pay != 'Not Provided' AND base_pay != '' \
                            GROUP BY year";

//            let sql = "SELECT MIN(to_float64(base_pay)), MAX(to_float64(base_pay)) \
//                            FROM salaries \
//                            WHERE base_pay != 'Not Provided' AND base_pay != ''";

            // create a data frame
            let df = ctx.sql(&sql).unwrap();

            ctx.write_csv(df, "_min_max_salaries.csv").unwrap();

        }
        _ => println!("Could not locate {} - try downloading it from https://www.kaggle.com/kaggle/sf-salaries/discussion/18736", path)
    }
}
