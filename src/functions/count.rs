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

//! Count() aggregate function

use std::rc::Rc;

use super::super::errors::*;
use super::super::types::*;

use arrow::array::*;
use arrow::datatypes::*;

pub struct CountFunction {
    data_type: DataType,
    count: usize,
}

impl CountFunction {
    pub fn new(data_type: &DataType) -> Self {
        CountFunction {
            data_type: data_type.clone(),
            count: 0,
        }
    }
}

impl AggregateFunction for CountFunction {
    fn name(&self) -> String {
        "COUNT".to_string()
    }

    fn args(&self) -> Vec<Field> {
        vec![Field::new("arg", self.data_type.clone(), true)]
    }

    fn return_type(&self) -> DataType {
        DataType::UInt64
    }

    fn execute(&mut self, args: &Vec<Rc<Value>>) -> Result<()> {
        assert_eq!(1, args.len());
        match args[0].as_ref() {
            Value::Column(ref array) => {
                self.count += array.len();
                Ok(())
            },
            Value::Scalar(_) => {
                self.count += 1;
                Ok(())
            }
        }
    }

    fn finish(&self) -> Result<Rc<Value>> {
        Ok(Rc::new(Value::Scalar(Rc::new(ScalarValue::UInt64(self.count as u64)))))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_count() {
        let mut count = CountFunction::new(&DataType::Float64);
        assert_eq!(DataType::UInt64, count.return_type());
        let values: Vec<f64> = vec![12.0, 22.0, 32.0, 6.0, 58.1];

        count.execute(&vec![Rc::new(Value::Column(Rc::new(Array::from(values))))])
            .unwrap();
        let result = count.finish().unwrap();

        match result.as_ref() {
            &Value::Scalar(ref v) => assert_eq!(v.get_u64().unwrap(), 5),
            _ => panic!(),
        }
    }
}
