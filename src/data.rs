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

/// The data types supported by this database. Currently just u64 and string but others
/// will be added later, including complex types
#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum DataType {
    Boolean,
    Float,
    Double,
    Int,
    UnsignedInt,
    Long,
    UnsignedLong,
    String,
    ComplexType(Vec<Field>)
}

/// Definition of a column in a relation (data set).
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
pub struct ComplexType {
    name: String,
    fields: Vec<Field>
}

#[derive(Debug)]
pub enum ColumnData {
    BroadcastVariable(Value),
    Boolean(Vec<bool>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    Int(Vec<i32>),
    UnsignedInt(Vec<u32>),
    Long(Vec<i64>),
    UnsignedLong(Vec<u64>),
    String(Vec<String>),
    ComplexValue(Vec<Rc<ColumnData>>)
}

impl ColumnData {

    pub fn len(&self) -> usize {
        match self {
            &ColumnData::BroadcastVariable(_) => 1,
            &ColumnData::Boolean(ref v) => v.len(),
            &ColumnData::Float(ref v) => v.len(),
            &ColumnData::Double(ref v) => v.len(),
            &ColumnData::Int(ref v) => v.len(),
            &ColumnData::UnsignedInt(ref v) => v.len(),
            &ColumnData::Long(ref v) => v.len(),
            &ColumnData::UnsignedLong(ref v) => v.len(),
            &ColumnData::String(ref v) => v.len(),
            &ColumnData::ComplexValue(ref v) => v[0].len(),
        }
    }

    pub fn eq(&self, other: &ColumnData) -> Vec<bool> {
        match (self, other) {
            // compare column to literal
            (&ColumnData::Float(ref l), &ColumnData::BroadcastVariable(Value::Float(b))) => l.iter().map(|a| a==&b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::BroadcastVariable(Value::Double(b))) => l.iter().map(|a| a==&b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::BroadcastVariable(Value::Int(b))) => l.iter().map(|a| a==&b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::BroadcastVariable(Value::UnsignedInt(b))) => l.iter().map(|a| a==&b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::BroadcastVariable(Value::Long(b))) => l.iter().map(|a| a==&b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::BroadcastVariable(Value::UnsignedLong(b))) => l.iter().map(|a| a==&b).collect(),
            (&ColumnData::String(ref l), &ColumnData::BroadcastVariable(Value::String(ref b))) => l.iter().map(|a| a==b).collect(),
            // compare column to column
            (&ColumnData::Float(ref l), &ColumnData::Float(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::Double(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::Int(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::UnsignedInt(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::Long(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::UnsignedLong(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            (&ColumnData::String(ref l), &ColumnData::String(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a==b).collect(),
            _ => panic!(format!("ColumnData.eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn not_eq(&self, other: &ColumnData) -> Vec<bool> {
        match (self, other) {
            // compare column to literal
            (&ColumnData::Float(ref l), &ColumnData::BroadcastVariable(Value::Float(b))) => l.iter().map(|a| a!=&b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::BroadcastVariable(Value::Double(b))) => l.iter().map(|a| a!=&b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::BroadcastVariable(Value::Int(b))) => l.iter().map(|a| a!=&b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::BroadcastVariable(Value::UnsignedInt(b))) => l.iter().map(|a| a!=&b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::BroadcastVariable(Value::Long(b))) => l.iter().map(|a| a!=&b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::BroadcastVariable(Value::UnsignedLong(b))) => l.iter().map(|a| a!=&b).collect(),
            (&ColumnData::String(ref l), &ColumnData::BroadcastVariable(Value::String(ref b))) => l.iter().map(|a| a!=b).collect(),
            // compare column to column
            (&ColumnData::Float(ref l), &ColumnData::Float(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::Double(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::Int(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::UnsignedInt(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::Long(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::UnsignedLong(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            (&ColumnData::String(ref l), &ColumnData::String(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a!=b).collect(),
            _ => panic!(format!("ColumnData.eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn lt(&self, other: &ColumnData) -> Vec<bool> {
        match (self, other) {
            // compare column to literal
            (&ColumnData::Float(ref l), &ColumnData::BroadcastVariable(Value::Float(b))) => l.iter().map(|a| a<&b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::BroadcastVariable(Value::Double(b))) => l.iter().map(|a| a<&b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::BroadcastVariable(Value::Int(b))) => l.iter().map(|a| a<&b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::BroadcastVariable(Value::UnsignedInt(b))) => l.iter().map(|a| a<&b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::BroadcastVariable(Value::Long(b))) => l.iter().map(|a| a<&b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::BroadcastVariable(Value::UnsignedLong(b))) => l.iter().map(|a| a<&b).collect(),
            (&ColumnData::String(ref l), &ColumnData::BroadcastVariable(Value::String(ref b))) => l.iter().map(|a| a<b).collect(),
            // compare column to column
            (&ColumnData::Float(ref l), &ColumnData::Float(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::Double(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::Int(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::UnsignedInt(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::Long(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::UnsignedLong(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            (&ColumnData::String(ref l), &ColumnData::String(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<b).collect(),
            _ => panic!(format!("ColumnData.lt() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn lt_eq(&self, other: &ColumnData) -> Vec<bool> {
        match (self, other) {
            // compare column to literal
            (&ColumnData::Float(ref l), &ColumnData::BroadcastVariable(Value::Float(b))) => l.iter().map(|a| a<=&b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::BroadcastVariable(Value::Double(b))) => l.iter().map(|a| a<=&b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::BroadcastVariable(Value::Int(b))) => l.iter().map(|a| a<=&b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::BroadcastVariable(Value::UnsignedInt(b))) => l.iter().map(|a| a<=&b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::BroadcastVariable(Value::Long(b))) => l.iter().map(|a| a<=&b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::BroadcastVariable(Value::UnsignedLong(b))) => l.iter().map(|a| a<=&b).collect(),
            (&ColumnData::String(ref l), &ColumnData::BroadcastVariable(Value::String(ref b))) => l.iter().map(|a| a<=b).collect(),
            // compare column to column
            (&ColumnData::Float(ref l), &ColumnData::Float(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::Double(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::Int(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::UnsignedInt(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::Long(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::UnsignedLong(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            (&ColumnData::String(ref l), &ColumnData::String(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a<=b).collect(),
            _ => panic!(format!("ColumnData.lt_eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn gt(&self, other: &ColumnData) -> Vec<bool> {
        match (self, other) {
            // compare column to literal
            (&ColumnData::Float(ref l), &ColumnData::BroadcastVariable(Value::Float(b))) => l.iter().map(|a| a>&b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::BroadcastVariable(Value::Double(b))) => l.iter().map(|a| a>&b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::BroadcastVariable(Value::Int(b))) => l.iter().map(|a| a>&b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::BroadcastVariable(Value::UnsignedInt(b))) => l.iter().map(|a| a>&b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::BroadcastVariable(Value::Long(b))) => l.iter().map(|a| a>&b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::BroadcastVariable(Value::UnsignedLong(b))) => l.iter().map(|a| a>&b).collect(),
            (&ColumnData::String(ref l), &ColumnData::BroadcastVariable(Value::String(ref b))) => l.iter().map(|a| a>b).collect(),
            // compare column to column
            (&ColumnData::Float(ref l), &ColumnData::Float(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::Double(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::Int(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::UnsignedInt(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::Long(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::UnsignedLong(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            (&ColumnData::String(ref l), &ColumnData::String(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>b).collect(),
            _ => panic!(format!("ColumnData.gt() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn gt_eq(&self, other: &ColumnData) -> Vec<bool> {
        match (self, other) {
            // compare column to literal
            (&ColumnData::Float(ref l), &ColumnData::BroadcastVariable(Value::Float(b))) => l.iter().map(|a| a>=&b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::BroadcastVariable(Value::Double(b))) => l.iter().map(|a| a>=&b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::BroadcastVariable(Value::Int(b))) => l.iter().map(|a| a>=&b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::BroadcastVariable(Value::UnsignedInt(b))) => l.iter().map(|a| a>=&b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::BroadcastVariable(Value::Long(b))) => l.iter().map(|a| a>=&b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::BroadcastVariable(Value::UnsignedLong(b))) => l.iter().map(|a| a>=&b).collect(),
            (&ColumnData::String(ref l), &ColumnData::BroadcastVariable(Value::String(ref b))) => l.iter().map(|a| a>=b).collect(),
            // compare column to column
            (&ColumnData::Float(ref l), &ColumnData::Float(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            (&ColumnData::Double(ref l), &ColumnData::Double(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            (&ColumnData::Int(ref l), &ColumnData::Int(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            (&ColumnData::UnsignedInt(ref l), &ColumnData::UnsignedInt(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            (&ColumnData::Long(ref l), &ColumnData::Long(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            (&ColumnData::UnsignedLong(ref l), &ColumnData::UnsignedLong(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            (&ColumnData::String(ref l), &ColumnData::String(ref r)) => l.iter().zip(r.iter()).map(|(a,b)| a>=b).collect(),
            _ => panic!(format!("ColumnData.gt_eq() Type mismatch: {:?} vs {:?}", self, other))
        }
    }

    pub fn get_value(&self, index: usize) -> Value {
//        println!("get_value() index={}", index);
        let v = match self {
            &ColumnData::BroadcastVariable(ref v) => v.clone(),
            &ColumnData::Boolean(ref v) => Value::Boolean(v[index]),
            &ColumnData::Float(ref v) => Value::Float(v[index]),
            &ColumnData::Double(ref v) => Value::Double(v[index]),
            &ColumnData::Int(ref v) => Value::Int(v[index]),
            &ColumnData::UnsignedInt(ref v) => Value::UnsignedInt(v[index]),
            &ColumnData::Long(ref v) => Value::Long(v[index]),
            &ColumnData::UnsignedLong(ref v) => Value::UnsignedLong(v[index]),
            &ColumnData::String(ref v) => Value::String(v[index].clone()),
            &ColumnData::ComplexValue(ref v) => {
                // v is Vec<ColumnData>
                // each field has its own ColumnData e.g. lat, lon so we want to get a value from each (but it's recursive)
                //            println!("get_value() complex value has {} fields", v.len());
                let fields = v.iter().map(|field| field.get_value(index)).collect();
                Value::ComplexValue(fields)
            }
        };
        //  println!("get_value() index={} returned {:?}", index, v);

        v
    }

    pub fn filter(&self, bools: &ColumnData) -> ColumnData {
        match bools {
            &ColumnData::Boolean(ref b) => match self {
                &ColumnData::Boolean(ref v) => ColumnData::Boolean(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect()),
                &ColumnData::Float(ref v) => ColumnData::Float(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect()),
                &ColumnData::Double(ref v) => ColumnData::Double(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect()),
                &ColumnData::Int(ref v) => ColumnData::Int(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect()),
                &ColumnData::UnsignedInt(ref v) => ColumnData::UnsignedInt(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect()),
                &ColumnData::Long(ref v) => ColumnData::Long(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect()),
                &ColumnData::UnsignedLong(ref v) => ColumnData::UnsignedLong(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| *v).collect()),
                &ColumnData::String(ref v) => ColumnData::String(v.iter().zip(b.iter()).filter(|&(_,f)| *f).map(|(v,_)| v.clone()).collect()),
                _ => unimplemented!()
            },
            _ => panic!()
        }
    }

}


/// Value holder for all supported data types
#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub enum Value {
    Boolean(bool),
    Float(f32),
    Double(f64),
    Int(i32),
    UnsignedInt(u32),
    Long(i64),
    UnsignedLong(u64),
    String(String),
    /// Complex value which is a list of values (which in turn can be complex
    /// values to support nested types)
    ComplexValue(Vec<Value>),
    /// values for user-defined types are stored as binary
    UserDefined(Vec<u8>)
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {

        //TODO: implement all type coercion rules

        match self {
            &Value::Double(l) => match other {
                &Value::Double(r) => l.partial_cmp(&r),
                &Value::Long(r) => l.partial_cmp(&(r as f64)),
                _ => unimplemented!("type coercion rules missing")
            },
            &Value::Long(l) => match other {
                &Value::Double(r) => (l as f64).partial_cmp(&r),
                &Value::Long(r) => l.partial_cmp(&r),
                _ => unimplemented!("type coercion rules missing")
            },
            &Value::String(ref l) => match other {
                &Value::String(ref r) => l.partial_cmp(r),
                _ => unimplemented!("type coercion rules missing")
            },
            &Value::ComplexValue(_) => None,
            _ => unimplemented!("type coercion rules missing")
        }

    }
}



impl Value {

    pub fn to_string(&self) -> String {
        match self {
            &Value::Long(l) => l.to_string(),
            &Value::UnsignedLong(l) => l.to_string(),
            &Value::Double(d) => d.to_string(),
            &Value::Boolean(b) => b.to_string(),
            &Value::String(ref s) => s.clone(),
            &Value::ComplexValue(ref v) => {
                let s : Vec<String> = v.iter()
                    .map(|v| v.to_string())
                    .collect();
                s.join(",")
            },
            _ => unimplemented!()
        }
    }

}
