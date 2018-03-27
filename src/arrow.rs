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

use std::iter::Iterator;
use std::rc::Rc;
use std::str;
use std::string::String;

extern crate bytes;
use self::bytes::{Bytes, BytesMut, BufMut};

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

pub struct ListData {
    //TODO: null bitmap
    pub offsets: Vec<i32>,
    pub bytes: Bytes
}

impl ListData {

    pub fn len(&self) -> usize {
        self.offsets.len()-1
    }

    pub fn slice(&self, index: usize) -> &[u8] {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index+1] as usize;
        &self.bytes[start..end]
    }
}

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
    Utf8(ListData),
    Struct(Vec<Rc<Array>>)
}


impl ArrayData {

    pub fn from_strings(s: Vec<String>) -> Self {
        let mut offsets : Vec<i32> = Vec::with_capacity(s.len() + 1);
        let mut buf = BytesMut::with_capacity(s.len() * 32);
        offsets.push(0_i32);
        s.iter().for_each(|v| {
            buf.put(v.as_bytes());
            offsets.push(buf.len() as i32);
        });

        ArrayData::Utf8(ListData { offsets, bytes: buf.freeze() })
    }

//    fn eq(l: &ArrayData, b: &ArrayData) -> ArrayData {
//
//    }

}

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
            &ArrayData::Utf8(ref list) => list.len(),
            &ArrayData::Struct(ref v) => v[0].as_ref().len(), // assumes all fields are same len
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_char() {

        let s = vec!["this", "is", "a", "test"];
        let mut offsets : Vec<i32> = Vec::with_capacity(s.len() + 1);

        let mut buf = BytesMut::with_capacity(64);
        assert_eq!(0, buf.len());

        offsets.push(0_i32);
        s.iter().for_each(|v| {
            buf.put(v);
            offsets.push(buf.len() as i32);
        });

        let x: Bytes = buf.freeze();

        assert_eq!(11, x.len());
        assert_eq!(0, offsets[0]);
        assert_eq!(4, offsets[1]);
        assert_eq!(6, offsets[2]);
        assert_eq!(7, offsets[3]);
        assert_eq!(11, offsets[4]);
    }
}



