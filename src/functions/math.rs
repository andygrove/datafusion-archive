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

//! Example math functions

use std::rc::Rc;

use super::super::errors::*;
use super::super::types::*;

//use arrow::array::*;
use arrow::datatypes::*;

pub struct SqrtFunction {}

impl ScalarFunction for SqrtFunction {
    fn name(&self) -> String {
        "sqrt".to_string()
    }

    fn execute(&self, args: Vec<Value>) -> Result<Value> {
        assert_eq!(1, args.len());
        match args[0] {
            Value::Column(ref arr) => match arr.data() {
                ArrayData::Float64(ref v) => Ok(Value::Column(Rc::new(Array::from(
                    v.iter().map(|v| v.sqrt()).collect::<Vec<f64>>(),
                )))),
                _ => Err(ExecutionError::General(
                    "Unsupported arg type for sqrt".to_string(),
                )),
            },
            Value::Scalar(ref v) => match v.as_ref() {
                ScalarValue::Float64(ref n) => {
                    Ok(Value::Scalar(Rc::new(ScalarValue::Float64(n.sqrt()))))
                }
                _ => Err(ExecutionError::General(
                    "Unsupported arg type for sqrt".to_string(),
                )),
            },
        }
    }

    fn args(&self) -> Vec<Field> {
        vec![Field::new("n", DataType::Float64, false)]
    }

    fn return_type(&self) -> DataType {
        DataType::Float64
    }
}
