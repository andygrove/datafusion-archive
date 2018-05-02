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

//! MIN() aggregate function

use std::rc::Rc;

use super::super::errors::*;
use super::super::types::*;

//use arrow::array::*;
use arrow::datatypes::*;

pub struct MinFunction {
    data_type: DataType,
    value: ScalarValue,
}

impl MinFunction {
    pub fn new(data_type: &DataType) -> Self {
        MinFunction {
            data_type: data_type.clone(),
            value: ScalarValue::Null,
        }
    }
}

impl AggregateFunction for MinFunction {
    fn name(&self) -> String {
        "MIN".to_string()
    }

    fn args(&self) -> Vec<Field> {
        vec![Field::new("arg", self.data_type.clone(), true)]
    }

    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn execute(&mut self, args: &Vec<Value>) -> Result<()> {
        assert_eq!(1, args.len());
        match args[0] {
            Value::Column(ref array) => {
                match array.data() {
                    //TODO support all types using macros
                    &ArrayData::Float64(ref buf) => {
                        for i in 0..buf.len() as usize {
                            let value = *buf.get(i);
                            match self.value {
                                ScalarValue::Null => self.value = ScalarValue::Float64(value),
                                ScalarValue::Float64(x) => if value < x {
                                    self.value = ScalarValue::Float64(value)
                                },
                                _ => panic!("type mismatch"),
                            }
                        }
                    }
                    _ => unimplemented!("unsupported data type in MinFunction"),
                }
                Ok(())
            }
            Value::Scalar(ref v) => match v.as_ref() {
                ScalarValue::Float64(ref value) => {
                    match self.value {
                        ScalarValue::Null => self.value = ScalarValue::Float64(*value),
                        ScalarValue::Float64(x) => if *value < x {
                            self.value = ScalarValue::Float64(*value)
                        },
                        _ => panic!("type mismatch"),
                    }
                    Ok(())
                }
                _ => unimplemented!("unsupported data type in MinFunction"),
            }
        }
    }

    fn finish(&self) -> Result<Value> {
        Ok(Value::Scalar(Rc::new(self.value.clone())))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_min() {
        let mut min = MinFunction::new(&DataType::Float64);
        assert_eq!(DataType::Float64, min.return_type());
        let values: Vec<f64> = vec![12.0, 22.0, 32.0, 6.0, 58.1];

        min.execute(&vec![Value::Column(Rc::new(Array::from(values)))])
            .unwrap();
        let result = min.finish().unwrap();

        match result {
            Value::Scalar(ref v) => assert_eq!(v.get_f64().unwrap(), 6.0),
            _ => panic!(),
        }
    }
}
