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

/// create a point from two doubles
pub struct STPointFunc;

impl ScalarFunction for STPointFunc {
    fn name(&self) -> String {
        "ST_Point".to_string()
    }

    fn execute(&self, args: Vec<Value>) -> Result<Value> {
        assert_eq!(2, args.len());
        match (&args[0], &args[1]) {
            (Value::Column(ref arr1), Value::Column(ref arr2)) => {
                match (arr1.data(), arr2.data()) {
                    (&ArrayData::Float64(_), &ArrayData::Float64(_)) => {
                        let nested: Vec<Rc<Array>> = vec![arr1.clone(), arr2.clone()];
                        let new_array = Array::new(arr1.len() as usize, ArrayData::Struct(nested));
                        Ok(Value::Column(Rc::new(new_array)))
                    }
                    _ => Err(ExecutionError::General(
                        "Unsupported type for ST_Point".to_string(),
                    )),
                }
            }
            _ => Err(ExecutionError::General(
                "Unsupported type for ST_Point".to_string(),
            )),
        }
    }

    fn args(&self) -> Vec<Field> {
        vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]
    }

    fn return_type(&self) -> DataType {
        DataType::Struct(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ])
    }
}
