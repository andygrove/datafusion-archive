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

//! MAX() aggregate function

use std::rc::Rc;
use std::str;

use super::super::errors::*;
use super::super::types::*;

//use arrow::array::*;
use arrow::datatypes::*;

pub struct MaxFunction {
    data_type: DataType,
    value: ScalarValue,
}

impl MaxFunction {
    pub fn new(data_type: &DataType) -> Self {
        MaxFunction {
            data_type: data_type.clone(),
            value: ScalarValue::Null,
        }
    }
}

macro_rules! max_in_column {
    ($SELF:ident, $BUF:ident, $VARIANT:ident) => {{
        for i in 0..$BUF.len() as usize {
            let value = *$BUF.get(i);
            match $SELF.value {
                ScalarValue::Null => $SELF.value = ScalarValue::$VARIANT(value),
                ScalarValue::$VARIANT(x) => if value > x {
                    $SELF.value = ScalarValue::$VARIANT(value)
                },
                ref other => panic!(
                    "Type mismatch in MAX() for datatype {} - {:?}",
                    stringify!($VARIANT),
                    other
                ),
            }
        }
    }};
}
macro_rules! max_in_scalar {
    ($SELF:ident, $VALUE:ident, $VARIANT:ident) => {{
        match $SELF.value {
            ScalarValue::Null => $SELF.value = ScalarValue::$VARIANT(*$VALUE),
            ScalarValue::$VARIANT(x) => if *$VALUE > x {
                $SELF.value = ScalarValue::$VARIANT(*$VALUE)
            },
            _ => panic!("Type mismatch in MAX()"),
        }
        Ok(())
    }};
}

impl AggregateFunction for MaxFunction {
    fn name(&self) -> String {
        "MAX".to_string()
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
                    ArrayData::Boolean(ref buf) => max_in_column!(self, buf, Boolean),
                    ArrayData::UInt8(ref buf) => max_in_column!(self, buf, UInt8),
                    ArrayData::UInt16(ref buf) => max_in_column!(self, buf, UInt16),
                    ArrayData::UInt32(ref buf) => max_in_column!(self, buf, UInt32),
                    ArrayData::UInt64(ref buf) => max_in_column!(self, buf, UInt64),
                    ArrayData::Int8(ref buf) => max_in_column!(self, buf, Int8),
                    ArrayData::Int16(ref buf) => max_in_column!(self, buf, Int16),
                    ArrayData::Int32(ref buf) => max_in_column!(self, buf, Int32),
                    ArrayData::Int64(ref buf) => max_in_column!(self, buf, Int64),
                    ArrayData::Float32(ref buf) => max_in_column!(self, buf, Float32),
                    ArrayData::Float64(ref buf) => max_in_column!(self, buf, Float64),
                    ArrayData::Utf8(ref list) => {
                        if list.len() > 0 {
                            let mut s = str::from_utf8(list.get(0)).unwrap().to_string();
                            for i in 1..list.len() {
                                let s2 = str::from_utf8(list.get(i)).unwrap().to_string();
                                if s2 < s {
                                    s = s2;
                                }
                            }
                            self.value = match &self.value {
                                ScalarValue::Null => ScalarValue::Utf8(Rc::new(s)),
                                ScalarValue::Utf8(current) => if &s < current.as_ref() {
                                    ScalarValue::Utf8(Rc::new(s))
                                } else {
                                    self.value.clone()
                                },
                                _ => panic!(),
                            };
                        }
                    }
                    ArrayData::Struct(_) => unimplemented!("MAX() does not support struct types"),
                }
                Ok(())
            }
            Value::Scalar(ref v) => match v.as_ref() {
                ScalarValue::UInt8(ref value) => max_in_scalar!(self, value, UInt8),
                ScalarValue::UInt16(ref value) => max_in_scalar!(self, value, UInt16),
                ScalarValue::UInt32(ref value) => max_in_scalar!(self, value, UInt32),
                ScalarValue::UInt64(ref value) => max_in_scalar!(self, value, UInt64),
                ScalarValue::Int8(ref value) => max_in_scalar!(self, value, Int8),
                ScalarValue::Int16(ref value) => max_in_scalar!(self, value, Int16),
                ScalarValue::Int32(ref value) => max_in_scalar!(self, value, Int32),
                ScalarValue::Int64(ref value) => max_in_scalar!(self, value, Int64),
                ScalarValue::Float32(ref value) => max_in_scalar!(self, value, Float32),
                ScalarValue::Float64(ref value) => max_in_scalar!(self, value, Float64),
                ScalarValue::Utf8(ref value) => {
                    self.value = match &self.value {
                        ScalarValue::Null => ScalarValue::Utf8(value.clone()),
                        ScalarValue::Utf8(ref current) => if value > current {
                            ScalarValue::Utf8(value.clone())
                        } else {
                            self.value.clone()
                        },
                        _ => panic!(),
                    };
                    Ok(())
                }
                _ => unimplemented!("MAX() unsupported scalar datatype"),
            },
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
    fn test_max() {
        let mut max = MaxFunction::new(&DataType::Float64);
        assert_eq!(DataType::Float64, max.return_type());
        let values: Vec<f64> = vec![12.0, 22.0, 32.0, 6.0, 58.1];

        max.execute(&vec![Value::Column(Rc::new(Array::from(values)))])
            .unwrap();
        let result = max.finish().unwrap();

        match result {
            Value::Scalar(ref v) => assert_eq!(v.get_f64().unwrap(), 58.1),
            _ => panic!(),
        }
    }
}
