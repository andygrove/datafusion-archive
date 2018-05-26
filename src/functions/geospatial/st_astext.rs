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

//use arrow::array::*;
use arrow::datatypes::*;

use super::super::super::errors::*;
use super::super::super::types::*;

/// Converts a point to Well-Known Text (WKT)
pub struct STAsText;

impl ScalarFunction for STAsText {
    fn name(&self) -> String {
        "ST_AsText".to_string()
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        assert_eq!(1, args.len());
        match args[0] {
            Value::Column(ref arr) => match arr.data() {
                &ArrayData::Struct(ref fields) => {
                    match (fields[0].as_ref().data(), fields[1].as_ref().data()) {
                        (&ArrayData::Float64(ref lat), &ArrayData::Float64(ref lon)) => {
                            let wkt: Vec<String> = lat
                                .iter()
                                .zip(lon.iter())
                                .map(|(lat2, lon2)| format!("POINT ({} {})", lat2, lon2))
                                .collect();
                            Ok(Value::Column(Rc::new(Array::from(wkt))))
                        }
                        _ => Err(ExecutionError::General(
                            "Unsupported type for ST_AsText".to_string(),
                        )),
                    }
                }
                _ => Err(ExecutionError::General(
                    "Unsupported type for ST_AsText".to_string(),
                )),
            },
            _ => Err(ExecutionError::General(
                "Unsupported type for ST_AsText".to_string(),
            )),
        }
    }

    fn args(&self) -> Vec<Field> {
        vec![Field::new(
            "point",
            DataType::Struct(vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
            ]),
            false,
        )]
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }
}
