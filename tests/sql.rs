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
use datafusion::execution::relation::Relation;

#[test]
fn csv_query_with_predicate() {
    let mut ctx = ExecutionContext::new();

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
    let sql = "SELECT city, lat, lng, lat + lng FROM cities WHERE lat > 51.0 AND lat < 53";

    // create a data frame
    let results = ctx.sql(&sql).unwrap();

    let actual = result_str(&results);
    println!("{}", actual);

    let expected= "\"Solihull, Birmingham, UK\"\t52.412811\t-1.778197\t50.634614\n\"Cardiff, Cardiff county, UK\"\t51.481583\t-3.17909\t48.302493\n\"Oxford, Oxfordshire, UK\"\t51.752022\t-1.257677\t50.494344999999996\n\"London, UK\"\t51.509865\t-0.118092\t51.391773\n\"Swindon, Swindon, UK\"\t51.568535\t-1.772232\t49.796302999999995\n\"Gravesend, Kent, UK\"\t51.441883\t0.370759\t51.812642\n\"Northampton, Northamptonshire, UK\"\t52.240479\t-0.902656\t51.337823\n\"Rugby, Warwickshire, UK\"\t52.370876\t-1.265032\t51.105844000000005\n\"Sutton Coldfield, West Midlands, UK\"\t52.570385\t-1.824042\t50.746343\n\"Harlow, Essex, UK\"\t51.772938\t0.10231\t51.875248000000006\n\"Swansea, Swansea, UK\"\t51.621441\t-3.943646\t47.677794999999996\n\"Salisbury, Wiltshire, UK\"\t51.068787\t-1.794472\t49.274315\n\"Wolverhampton, West Midlands, UK\"\t52.59137\t-2.110748\t50.480622\n\"Bedford, UK\"\t52.136436\t-0.460739\t51.67569700000001\n\"Basildon, Essex, UK\"\t51.572376\t0.470009\t52.042384999999996\n\"Chippenham, Wiltshire, UK\"\t51.458057\t-2.116074\t49.341983\n\"Haverhill, Suffolk, UK\"\t52.080875\t0.444517\t52.525392\n\"Frankton, Warwickshire, UK\"\t52.328415\t-1.377561\t50.950854\n".to_string();

    assert_eq!(expected, actual);
}

fn result_str(results: &Rc<RefCell<Relation>>) -> String {
    let mut relation = results.borrow_mut();
    let mut str = String::new();
    while let Some(batch) = relation.next().unwrap() {
        for row_index in 0..batch.num_rows() {
            for column_index in 0..batch.num_columns() {
                if column_index > 0 {
                    str.push_str("\t");
                }
                let column = batch.column(column_index);

                match column.data_type() {
                    DataType::Float64 => {
                        let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        str.push_str(&format!("{:?}", array.value(row_index)));
                    }
                    DataType::Utf8 => {
                        let array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
                        let s = String::from_utf8(array.get_value(row_index).to_vec()).unwrap();

                        str.push_str(&format!("{:?}", s));
                    }
                    _ => str.push_str("???"),
                }
            }
            str.push_str("\n");
        }
    }
    str
}
