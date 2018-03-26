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

use std::clone::Clone;
use std::iter::Iterator;
use std::rc::Rc;
use std::str;
use std::string::String;
use std::cmp::{Ordering, PartialOrd};

//
// Warning! The type system is now loosely based on Apache Arrow but is not yet compatible with
// Apache Arrow. This is a work-in-progress.
//

#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum TimeUnit {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Timestamp(TimeUnit),
    Time(TimeUnit),
    Date32,
    Date64,
    Utf8,
    Binary,
    List(Vec<DataType>),
    Struct(Vec<Field>)
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool
}

impl Field {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.to_string(),
            data_type: data_type,
            nullable: nullable
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}: {:?}", self.name, self.data_type)
    }
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct Schema {
    pub columns: Vec<Field>
}

impl Schema {

    /// create an empty schema
    pub fn empty() -> Self { Schema { columns: vec![] } }

    pub fn new(columns: Vec<Field>) -> Self { Schema { columns: columns } }

    /// look up a column by name and return a reference to the column along with it's index
    pub fn column(&self, name: &str) -> Option<(usize, &Field)> {
        self.columns.iter()
            .enumerate()
            .find(|&(_,c)| c.name == name)
    }

    pub fn to_string(&self) -> String {
        let s : Vec<String> = self.columns.iter()
            .map(|c| c.to_string())
            .collect();
        s.join(",")
    }

}


#[derive(Debug)]
pub enum ArrayData {
    Boolean(Vec<bool>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Utf8(Vec<String>), // not compatible with Arrow
    Struct(Vec<Rc<Array>>)
}

#[derive(Debug)]
pub struct Array {
    //TODO: add null bitmap
    data: ArrayData
}

impl Array {

    pub fn new(data: ArrayData) -> Self {
        Array { data }
    }

    pub fn data(&self) -> &ArrayData {
        &self.data
    }

    pub fn len(&self) -> usize {
        match &self.data {
            &ArrayData::Boolean(ref v) => v.len(),
            &ArrayData::Float32(ref v) => v.len(),
            &ArrayData::Float64(ref v) => v.len(),
            &ArrayData::Int8(ref v) => v.len(),
            &ArrayData::Int16(ref v) => v.len(),
            &ArrayData::Int32(ref v) => v.len(),
            &ArrayData::Int64(ref v) => v.len(),
            &ArrayData::UInt8(ref v) => v.len(),
            &ArrayData::UInt16(ref v) => v.len(),
            &ArrayData::UInt32(ref v) => v.len(),
            &ArrayData::UInt64(ref v) => v.len(),
            &ArrayData::Utf8(ref v) => v.len(),
            &ArrayData::Struct(ref v) => v[0].as_ref().len(),
        }
    }

    pub fn eq_scalar(&self, other: &ScalarValue) -> Vec<bool> {
        match (&self.data, other) {
            (&ArrayData::Float32(ref l), &ScalarValue::Float32(b)) => l.iter().map(|a| a==&b).collect(),
            (&ArrayData::Float64(ref l), &ScalarValue::Float64(b)) => l.iter().map(|a| a==&b).collect(),
            (&ArrayData::Int32(ref l), &ScalarValue::Int32(b)) => l.iter().map(|a| a==&b).collect(),
            (&ArrayData::Int64(ref l), &ScalarValue::Int64(b)) => l.iter().map(|a| a==&b).collect(),
            (&ArrayData::Utf8(ref l), &ScalarValue::Utf8(ref b)) => l.iter().map(|a| a==b).collect(),
            _ => panic!(format!("ArrayData.eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn eq(&self, other: &Array) -> Vec<bool> {
        match (&self.data, &other.data) {
            (&ArrayData::Float32(ref l), &ArrayData::Float32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            (&ArrayData::Float64(ref l), &ArrayData::Float64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            (&ArrayData::Int32(ref l), &ArrayData::Int32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            (&ArrayData::Int64(ref l), &ArrayData::Int64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            (&ArrayData::Utf8(ref l), &ArrayData::Utf8(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            _ => panic!(format!("ArrayData.eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn not_eq_scalar(&self, other: &ScalarValue) -> Vec<bool> {
        match (&self.data, other) {
            (&ArrayData::Float32(ref l), &ScalarValue::Float32(b)) => l.iter().map(|a| a!=&b).collect(),
            (&ArrayData::Float64(ref l), &ScalarValue::Float64(b)) => l.iter().map(|a| a!=&b).collect(),
            (&ArrayData::Int32(ref l), &ScalarValue::Int32(b)) => l.iter().map(|a| a!=&b).collect(),
            (&ArrayData::Int64(ref l), &ScalarValue::Int64(b)) => l.iter().map(|a| a!=&b).collect(),
            (&ArrayData::Utf8(ref l), &ScalarValue::Utf8(ref b)) => l.iter().map(|a| a!=b).collect(),
            _ => panic!(format!("ArrayData.eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn not_eq(&self, other: &Array) -> Vec<bool> {
        match (&self.data, &other.data) {
            (&ArrayData::Float32(ref l), &ArrayData::Float32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            (&ArrayData::Float64(ref l), &ArrayData::Float64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            (&ArrayData::Int32(ref l), &ArrayData::Int32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            (&ArrayData::Int64(ref l), &ArrayData::Int64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            (&ArrayData::Utf8(ref l), &ArrayData::Utf8(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            _ => panic!(format!("ArrayData.eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn lt_scalar(&self, other: &ScalarValue) -> Vec<bool> {
        match (&self.data, other) {
            (&ArrayData::Float32(ref l), &ScalarValue::Float32(b)) => l.iter().map(|a| a<&b).collect(),
            (&ArrayData::Float64(ref l), &ScalarValue::Float64(b)) => l.iter().map(|a| a<&b).collect(),
            (&ArrayData::Int32(ref l), &ScalarValue::Int32(b)) => l.iter().map(|a| a<&b).collect(),
            (&ArrayData::Int64(ref l), &ScalarValue::Int64(b)) => l.iter().map(|a| a<&b).collect(),
            (&ArrayData::Utf8(ref l), &ScalarValue::Utf8(ref b)) => l.iter().map(|a| a<b).collect(),
            _ => panic!(format!("ArrayData.lt() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn lt(&self, other: &Array) -> Vec<bool> {
        match (&self.data, &other.data) {
            (&ArrayData::Float32(ref l), &ArrayData::Float32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            (&ArrayData::Float64(ref l), &ArrayData::Float64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            (&ArrayData::Int32(ref l), &ArrayData::Int32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            (&ArrayData::Int64(ref l), &ArrayData::Int64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            (&ArrayData::Utf8(ref l), &ArrayData::Utf8(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            _ => panic!(format!("ArrayData.lt() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn lt_eq_scalar(&self, other: &ScalarValue) -> Vec<bool> {
        match (&self.data, other) {
            (&ArrayData::Float32(ref l), &ScalarValue::Float32(b)) => l.iter().map(|a| a<=&b).collect(),
            (&ArrayData::Float64(ref l), &ScalarValue::Float64(b)) => l.iter().map(|a| a<=&b).collect(),
            (&ArrayData::Int32(ref l), &ScalarValue::Int32(b)) => l.iter().map(|a| a<=&b).collect(),
            (&ArrayData::Int64(ref l), &ScalarValue::Int64(b)) => l.iter().map(|a| a<=&b).collect(),
            (&ArrayData::Utf8(ref l), &ScalarValue::Utf8(ref b)) => l.iter().map(|a| a<=b).collect(),
            _ => panic!(format!("ArrayData.lt_eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn lt_eq(&self, other: &Array) -> Vec<bool> {
        match (&self.data, &other.data) {
            (&ArrayData::Float32(ref l), &ArrayData::Float32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            (&ArrayData::Float64(ref l), &ArrayData::Float64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            (&ArrayData::Int32(ref l), &ArrayData::Int32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            (&ArrayData::Int64(ref l), &ArrayData::Int64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            (&ArrayData::Utf8(ref l), &ArrayData::Utf8(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            _ => panic!(format!("ArrayData.lt_eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn gt_scalar(&self, other: &ScalarValue) -> Vec<bool> {
        match (&self.data, other) {
            (&ArrayData::Float32(ref l), &ScalarValue::Float32(b)) => l.iter().map(|a| a>&b).collect(),
            (&ArrayData::Float64(ref l), &ScalarValue::Float64(b)) => l.iter().map(|a| a>&b).collect(),
            (&ArrayData::Int32(ref l), &ScalarValue::Int32(b)) => l.iter().map(|a| a>&b).collect(),
            (&ArrayData::Int64(ref l), &ScalarValue::Int64(b)) => l.iter().map(|a| a>&b).collect(),
            (&ArrayData::Utf8(ref l), &ScalarValue::Utf8(ref b)) => l.iter().map(|a| a>b).collect(),
            _ => panic!(format!("ArrayData.gt() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn gt(&self, other: &Array) -> Vec<bool> {
        match (&self.data, &other.data) {
            (&ArrayData::Float32(ref l), &ArrayData::Float32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            (&ArrayData::Float64(ref l), &ArrayData::Float64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            (&ArrayData::Int32(ref l), &ArrayData::Int32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            (&ArrayData::Int64(ref l), &ArrayData::Int64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            (&ArrayData::Utf8(ref l), &ArrayData::Utf8(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            _ => panic!(format!("ArrayData.gt() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn gt_eq_scalar(&self, other: &ScalarValue) -> Vec<bool> {
        match (&self.data, other) {
            (&ArrayData::Float32(ref l), &ScalarValue::Float32(b)) => l.iter().map(|a| a>=&b).collect(),
            (&ArrayData::Float64(ref l), &ScalarValue::Float64(b)) => l.iter().map(|a| a>=&b).collect(),
            (&ArrayData::Int32(ref l), &ScalarValue::Int32(b)) => l.iter().map(|a| a>=&b).collect(),
            (&ArrayData::Int64(ref l), &ScalarValue::Int64(b)) => l.iter().map(|a| a>=&b).collect(),
            (&ArrayData::Utf8(ref l), &ScalarValue::Utf8(ref b)) => l.iter().map(|a| a>=b).collect(),
            _ => panic!(format!("ArrayData.gt_eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn gt_eq(&self, other: &Array) -> Vec<bool> {
        match (&self.data, &other.data) {
            (&ArrayData::Float32(ref l), &ArrayData::Float32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            (&ArrayData::Float64(ref l), &ArrayData::Float64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            (&ArrayData::Int32(ref l), &ArrayData::Int32(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            (&ArrayData::Int64(ref l), &ArrayData::Int64(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            (&ArrayData::Utf8(ref l), &ArrayData::Utf8(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            _ => panic!(format!("ArrayData.gt_eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn get_value(&self, index: usize) -> ScalarValue {
//        println!("get_value() index={}", index);
        let v = match &self.data {
            &ArrayData::Boolean(ref v) => ScalarValue::Boolean(v[index]),
            &ArrayData::Float32(ref v) => ScalarValue::Float32(v[index]),
            &ArrayData::Float64(ref v) => ScalarValue::Float64(v[index]),
            &ArrayData::Int8(ref v) => ScalarValue::Int8(v[index]),
            &ArrayData::Int16(ref v) => ScalarValue::Int16(v[index]),
            &ArrayData::Int32(ref v) => ScalarValue::Int32(v[index]),
            &ArrayData::Int64(ref v) => ScalarValue::Int64(v[index]),
            &ArrayData::UInt8(ref v) => ScalarValue::UInt8(v[index]),
            &ArrayData::UInt16(ref v) => ScalarValue::UInt16(v[index]),
            &ArrayData::UInt32(ref v) => ScalarValue::UInt32(v[index]),
            &ArrayData::UInt64(ref v) => ScalarValue::UInt64(v[index]),
            &ArrayData::Utf8(ref v) => ScalarValue::Utf8(v[index].clone()),
            &ArrayData::Struct(ref v) => {
                // v is Vec<ArrayData>
                // each field has its own ArrayData e.g. lat, lon so we want to get a value from each (but it's recursive)
                //            println!("get_value() complex value has {} fields", v.len());
                let fields = v.iter().map(|field| field.get_value(index)).collect();
                ScalarValue::Struct(fields)
            }
        };
        //  println!("get_value() index={} returned {:?}", index, v);

        v
    }

    pub fn filter(&self, bools: &Array) -> Array{
        match bools.data() {
            &ArrayData::Boolean(ref b) => match &self.data {
                &ArrayData::Boolean(ref v) => Array::new(ArrayData::Boolean(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect())),
                &ArrayData::Float32(ref v) => Array::new(ArrayData::Float32(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect())),
                &ArrayData::Float64(ref v) => Array::new(ArrayData::Float64(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect())),
                &ArrayData::Int32(ref v) => Array::new(ArrayData::Int32(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect())),
                &ArrayData::Int64(ref v) => Array::new(ArrayData::Int64(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect())),
                &ArrayData::Utf8(ref v) => Array::new(ArrayData::Utf8(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| v.clone()).collect())),
                _ => unimplemented!()
            },
            _ => panic!()
        }
    }

}


/// Value holder for all supported data types
#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub enum ScalarValue {
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

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &ScalarValue) -> Option<Ordering> {

        //TODO: implement all type coercion rules

        match self {
            &ScalarValue::Float64(l) => match other {
                &ScalarValue::Float64(r) => l.partial_cmp(&r),
                &ScalarValue::Int64(r) => l.partial_cmp(&(r as f64)),
                _ => unimplemented!("type coercion rules missing")
            },
            &ScalarValue::Int64(l) => match other {
                &ScalarValue::Float64(r) => (l as f64).partial_cmp(&r),
                &ScalarValue::Int64(r) => l.partial_cmp(&r),
                _ => unimplemented!("type coercion rules missing")
            },
            &ScalarValue::Utf8(ref l) => match other {
                &ScalarValue::Utf8(ref r) => l.partial_cmp(r),
                _ => unimplemented!("type coercion rules missing")
            },
            &ScalarValue::Struct(_) => None,
            _ => unimplemented!("type coercion rules missing")
        }

    }
}


impl ScalarValue {

    pub fn to_string(&self) -> String {
        match self {
            &ScalarValue::Boolean(b) => b.to_string(),
            &ScalarValue::Int8(l) => l.to_string(),
            &ScalarValue::Int16(l) => l.to_string(),
            &ScalarValue::Int32(l) => l.to_string(),
            &ScalarValue::Int64(l) => l.to_string(),
            &ScalarValue::UInt8(l) => l.to_string(),
            &ScalarValue::UInt16(l) => l.to_string(),
            &ScalarValue::UInt32(l) => l.to_string(),
            &ScalarValue::UInt64(l) => l.to_string(),
            &ScalarValue::Float32(d) => d.to_string(),
            &ScalarValue::Float64(d) => d.to_string(),
            &ScalarValue::Utf8(ref s) => s.clone(),
            &ScalarValue::Struct(ref v) => {
                let s : Vec<String> = v.iter()
                    .map(|v| v.to_string())
                    .collect();
                s.join(",")
            }
        }
    }

}
