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

//! Datatype definitions

use std::fmt;
use std::fmt::Formatter;
use std::rc::Rc;
use std::result;

use arrow::array::Array;
use arrow::datatypes::{DataType, Field};

use super::errors::*;

/// ScalarValue enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Float32(f32),
    Float64(f64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Utf8(Rc<String>),
    Struct(Vec<ScalarValue>),
}

macro_rules! primitive_accessor {
    ($NAME:ident, $VARIANT:ident, $TY:ty) => {
        pub fn $NAME(&self) -> Result<$TY> {
            match *self {
                ScalarValue::$VARIANT(v) => Ok(v),
                _ => Err(df_error!("type mismatch".to_string()))
            }
        }
    }
}

impl ScalarValue {
    primitive_accessor!(get_bool, Boolean, bool);
    primitive_accessor!(get_i8, Int8, i8);
    primitive_accessor!(get_i16, Int16, i16);
    primitive_accessor!(get_i32, Int32, i32);
    primitive_accessor!(get_i64, Int64, i64);
    primitive_accessor!(get_u8, UInt8, u8);
    primitive_accessor!(get_u16, UInt16, u16);
    primitive_accessor!(get_u32, UInt32, u32);
    primitive_accessor!(get_u64, UInt64, u64);
    primitive_accessor!(get_f32, Float32, f32);
    primitive_accessor!(get_f64, Float64, f64);

    pub fn get_string(&self) -> Result<&String> {
        match *self {
            ScalarValue::Utf8(ref v) => Ok(v),
            _ => Err(df_error!("TBD")),
        }
    }

    pub fn get_struct(&self) -> Result<&Vec<ScalarValue>> {
        match *self {
            ScalarValue::Struct(ref v) => Ok(v),
            _ => Err(df_error!("TBD")),
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ScalarValue::Null => write!(f, "NULL"),
            &ScalarValue::Boolean(v) => write!(f, "{}", v),
            &ScalarValue::Int8(v) => write!(f, "{}", v),
            &ScalarValue::Int16(v) => write!(f, "{}", v),
            &ScalarValue::Int32(v) => write!(f, "{}", v),
            &ScalarValue::Int64(v) => write!(f, "{}", v),
            &ScalarValue::UInt8(v) => write!(f, "{}", v),
            &ScalarValue::UInt16(v) => write!(f, "{}", v),
            &ScalarValue::UInt32(v) => write!(f, "{}", v),
            &ScalarValue::UInt64(v) => write!(f, "{}", v),
            &ScalarValue::Float32(v) => write!(f, "{}", v),
            &ScalarValue::Float64(v) => write!(f, "{}", v),
            &ScalarValue::Utf8(ref v) => write!(f, "{}", v),
            &ScalarValue::Struct(ref v) => {
                for i in 0..v.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v[i])?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone)]
pub enum Value {
    Column(Rc<Array>),
    Scalar(Rc<ScalarValue>),
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Value::Scalar(v) => write!(f, "{:?}", v)?,
            Value::Column(ref array) => write!(f, "[array with length {}]", array.len())?,
        }
        Ok(())
    }
}

/// Scalar function
pub trait ScalarFunction {
    fn name(&self) -> String;
    fn args(&self) -> Vec<Field>;
    fn return_type(&self) -> DataType;
    fn execute(&self, args: Vec<Value>) -> Result<Value>;
}

/// Aggregate function
pub trait AggregateFunction {
    fn name(&self) -> String;
    fn args(&self) -> Vec<Field>;
    fn return_type(&self) -> DataType;
    fn execute(&mut self, args: &Vec<Value>) -> Result<()>;
    fn finish(&self) -> Result<Value>;
}
