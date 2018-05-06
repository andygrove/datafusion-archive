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

use arrow::bitmap::*;
use arrow::buffer::*;
use arrow::datatypes::{DataType, Field};
use arrow::list::*;

use super::errors::*;

pub struct Array {
    /// number of elements in the array
    len: i32,
    /// number of null elements in the array
    null_count: i32,
    /// If null_count is greater than zero then the validity_bitmap will be Some(Bitmap)
    validity_bitmap: Option<Bitmap>,
    /// The array of elements
    data: ArrayData,
}

impl Array {
    /// Create a new array where there are no null values
    pub fn new(len: usize, data: ArrayData) -> Self {
        Array {
            len: len as i32,
            data,
            validity_bitmap: None,
            null_count: 0,
        }
    }

    /// Create a new array where there are no null values
    pub fn with_nulls(len: usize, data: ArrayData, null_count: usize, bitmap: Bitmap) -> Self {
        Array {
            len: len as i32,
            data,
            validity_bitmap: Some(bitmap),
            null_count: null_count as i32
        }
    }

    /// Get a reference to the array data
    pub fn data(&self) -> &ArrayData {
        &self.data
    }

    /// number of elements in the array
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// number of null elements in the array
    pub fn null_count(&self) -> usize {
        self.null_count as usize
    }

    /// If null_count is greater than zero then the validity_bitmap will be Some(Bitmap)
    pub fn validity_bitmap(&self) -> &Option<Bitmap> {
        &self.validity_bitmap
    }
}

macro_rules! arraydata_from_primitive {
    ($DT:ty, $AT:ident) => {
        impl From<Vec<$DT>> for ArrayData {
            fn from(v: Vec<$DT>) -> Self {
                ArrayData::$AT(Buffer::from(v))
            }
        }
        impl From<Buffer<$DT>> for ArrayData {
            fn from(v: Buffer<$DT>) -> Self {
                ArrayData::$AT(v)
            }
        }
    };
}

arraydata_from_primitive!(bool, Boolean);
arraydata_from_primitive!(f32, Float32);
arraydata_from_primitive!(f64, Float64);
arraydata_from_primitive!(i8, Int8);
arraydata_from_primitive!(i16, Int16);
arraydata_from_primitive!(i32, Int32);
arraydata_from_primitive!(i64, Int64);
arraydata_from_primitive!(u8, UInt8);
arraydata_from_primitive!(u16, UInt16);
arraydata_from_primitive!(u32, UInt32);
arraydata_from_primitive!(u64, UInt64);

pub enum ArrayData {
    Boolean(Buffer<bool>),
    Float32(Buffer<f32>),
    Float64(Buffer<f64>),
    Int8(Buffer<i8>),
    Int16(Buffer<i16>),
    Int32(Buffer<i32>),
    Int64(Buffer<i64>),
    UInt8(Buffer<u8>),
    UInt16(Buffer<u16>),
    UInt32(Buffer<u32>),
    UInt64(Buffer<u64>),
    Utf8(List<u8>),
    Struct(Vec<Rc<Array>>),
}

macro_rules! array_from_primitive {
    ($DT:ty) => {
        impl From<Vec<$DT>> for Array {
            fn from(v: Vec<$DT>) -> Self {
                Array {
                    len: v.len() as i32,
                    null_count: 0,
                    validity_bitmap: None,
                    data: ArrayData::from(v),
                }
            }
        }
        impl From<Buffer<$DT>> for Array {
            fn from(v: Buffer<$DT>) -> Self {
                Array {
                    len: v.len() as i32,
                    null_count: 0,
                    validity_bitmap: None,
                    data: ArrayData::from(v),
                }
            }
        }
    };
}

array_from_primitive!(bool);
array_from_primitive!(f32);
array_from_primitive!(f64);
array_from_primitive!(u8);
array_from_primitive!(u16);
array_from_primitive!(u32);
array_from_primitive!(u64);
array_from_primitive!(i8);
array_from_primitive!(i16);
array_from_primitive!(i32);
array_from_primitive!(i64);

/// This method mostly just used for unit tests
impl From<Vec<&'static str>> for Array {
    fn from(v: Vec<&'static str>) -> Self {
        Array::from(v.iter().map(|s| s.to_string()).collect::<Vec<String>>())
    }
}

impl From<Vec<String>> for Array {
    fn from(v: Vec<String>) -> Self {
        Array {
            len: v.len() as i32,
            null_count: 0,
            validity_bitmap: None,
            data: ArrayData::Utf8(List::from(v)),
        }
    }
}

impl From<Vec<Rc<Array>>> for Array {
    fn from(v: Vec<Rc<Array>>) -> Self {
        Array {
            len: v.len() as i32,
            null_count: 0,
            validity_bitmap: None,
            data: ArrayData::Struct(v.iter().map(|a| a.clone()).collect()),
        }
    }
}

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

impl ScalarValue {
    pub fn get_datatype(&self) -> DataType {
        match *self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Struct(_) => unimplemented!(),
            ScalarValue::Null => unimplemented!()
        }
    }
}

macro_rules! primitive_accessor {
    ($NAME:ident, $VARIANT:ident, $TY:ty) => {
        pub fn $NAME(&self) -> Result<$TY> {
            match self {
                ScalarValue::$VARIANT(v) => Ok(*v),
                other => Err(ExecutionError::General(format!("Cannot access scalar value {:?} as {}", other, stringify!($VARIANT))))
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
