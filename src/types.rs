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
use std::io::Error;
use std::rc::Rc;

use arrow::array::Array;
use arrow::datatypes::{DataType, Field};

use super::sqlparser::ParserError;

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
    Utf8(String),
    Struct(Vec<ScalarValue>),
}

macro_rules! primitive_accessor {
    ($NAME:ident, $VARIANT:ident, $TY:ty) => {
        pub fn $NAME(&self) -> Result<$TY, ()> {
            match *self {
                ScalarValue::$VARIANT(v) => Ok(v),
                _ => Err(())
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

    pub fn get_string(&self) -> Result<&String, ()> {
        match *self {
            ScalarValue::Utf8(ref v) => Ok(v),
            _ => Err(()),
        }
    }

    pub fn get_struct(&self) -> Result<&Vec<ScalarValue>, ()> {
        match *self {
            ScalarValue::Struct(ref v) => Ok(v),
            _ => Err(()),
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
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            Value::Scalar(v) => write!(f, "{:?}", v)?,
            _ => write!(f, "???")?,
        }
        Ok(())
    }
}

/// Scalar function
pub trait ScalarFunction {
    fn name(&self) -> String;
    fn args(&self) -> Vec<Field>;
    fn return_type(&self) -> DataType;
    fn execute(&self, args: Vec<Rc<Value>>) -> Result<Rc<Value>, ExecutionError>;
}

/// Aggregate function
pub trait AggregateFunction {
    fn name(&self) -> String;
    fn args(&self) -> Vec<Field>;
    fn return_type(&self) -> DataType;
    fn execute(&mut self, args: Vec<Rc<Value>>) -> Result<(), ExecutionError>;
    fn finish(&self) -> Result<Rc<Value>, ExecutionError>;
}

#[derive(Debug)]
pub enum ExecutionError {
    IoError(Error),
    ParserError(ParserError),
    Custom(String),
    InvalidColumn(String),
    NotImplemented,
}

impl From<Error> for ExecutionError {
    fn from(e: Error) -> Self {
        ExecutionError::IoError(e)
    }
}

impl From<String> for ExecutionError {
    fn from(e: String) -> Self {
        ExecutionError::Custom(e)
    }
}

impl From<ParserError> for ExecutionError {
    fn from(e: ParserError) -> Self {
        ExecutionError::ParserError(e)
    }
}
