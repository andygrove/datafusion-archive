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

use std::fmt;
use std::ops::Add;
use std::rc::Rc;

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::DataType;

use super::error::{ExecutionError, Result};

/// Value enumeration
pub enum Value {
    ScalarValue(ScalarValue),
    ArrowValue(ArrowValue),
}

pub enum ArrowValue {
    Int32Array(Int32Array),
    Int64Array(Int64Array),
    //TODO add others
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
            ScalarValue::Null => unimplemented!(),
        }
    }
}

pub fn can_coerce_from(left: &DataType, other: &DataType) -> bool {
    use self::DataType::*;
    match left {
        Int8 => match other {
            Int8 => true,
            _ => false,
        },
        Int16 => match other {
            Int8 | Int16 => true,
            _ => false,
        },
        Int32 => match other {
            Int8 | Int16 | Int32 => true,
            _ => false,
        },
        Int64 => match other {
            Int8 | Int16 | Int32 | Int64 => true,
            _ => false,
        },
        UInt8 => match other {
            UInt8 => true,
            _ => false,
        },
        UInt16 => match other {
            UInt8 | UInt16 => true,
            _ => false,
        },
        UInt32 => match other {
            UInt8 | UInt16 | UInt32 => true,
            _ => false,
        },
        UInt64 => match other {
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            _ => false,
        },
        Float32 => match other {
            Int8 | Int16 | Int32 | Int64 => true,
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            Float32 => true,
            _ => false,
        },
        Float64 => match other {
            Int8 | Int16 | Int32 | Int64 => true,
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            Float32 | Float64 => true,
            _ => false,
        },
        _ => false,
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
            _ => Err(ExecutionError::from("TBD")),
        }
    }

    pub fn get_struct(&self) -> Result<&Vec<ScalarValue>> {
        match *self {
            ScalarValue::Struct(ref v) => Ok(v),
            _ => Err(ExecutionError::from("TBD")),
        }
    }
}

impl Add for ScalarValue {
    type Output = ScalarValue;

    fn add(self, rhs: ScalarValue) -> ScalarValue {
        assert_eq!(self.get_datatype(), rhs.get_datatype());
        match self {
            ScalarValue::UInt8(x) => ScalarValue::UInt8(x + rhs.get_u8().unwrap()),
            ScalarValue::UInt16(x) => ScalarValue::UInt16(x + rhs.get_u16().unwrap()),
            ScalarValue::UInt32(x) => ScalarValue::UInt32(x + rhs.get_u32().unwrap()),
            ScalarValue::UInt64(x) => ScalarValue::UInt64(x + rhs.get_u64().unwrap()),
            ScalarValue::Float32(x) => ScalarValue::Float32(x + rhs.get_f32().unwrap()),
            ScalarValue::Float64(x) => ScalarValue::Float64(x + rhs.get_f64().unwrap()),
            ScalarValue::Int8(x) => ScalarValue::Int8(x.saturating_add(rhs.get_i8().unwrap())),
            ScalarValue::Int16(x) => ScalarValue::Int16(x.saturating_add(rhs.get_i16().unwrap())),
            ScalarValue::Int32(x) => ScalarValue::Int32(x.saturating_add(rhs.get_i32().unwrap())),
            ScalarValue::Int64(x) => ScalarValue::Int64(x.saturating_add(rhs.get_i64().unwrap())),
            _ => panic!("Unsupported type for addition"),
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Boolean(v) => write!(f, "{}", v),
            ScalarValue::Int8(v) => write!(f, "{}", v),
            ScalarValue::Int16(v) => write!(f, "{}", v),
            ScalarValue::Int32(v) => write!(f, "{}", v),
            ScalarValue::Int64(v) => write!(f, "{}", v),
            ScalarValue::UInt8(v) => write!(f, "{}", v),
            ScalarValue::UInt16(v) => write!(f, "{}", v),
            ScalarValue::UInt32(v) => write!(f, "{}", v),
            ScalarValue::UInt64(v) => write!(f, "{}", v),
            ScalarValue::Float32(v) => write!(f, "{}", v),
            ScalarValue::Float64(v) => write!(f, "{}", v),
            ScalarValue::Utf8(ref v) => write!(f, "{}", v),
            ScalarValue::Struct(ref v) => {
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
