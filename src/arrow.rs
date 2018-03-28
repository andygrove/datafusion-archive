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

use std::convert::From;
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
    Utf8,
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

pub struct Bitmap {
    bits: Vec<u8>
}

impl Bitmap {

    pub fn new(n: usize) -> Self {
        let r = n % 64;
        let len = if r==0 { n } else { n + 64-r };
        let mut v = Vec::with_capacity(len);
        for _ in 0 .. len {
            v.push(0);
        }
        Bitmap { bits: v }
    }

    pub fn len(&self) -> usize {
        self.bits.len()
    }

    pub fn is_set(&self, i: usize) -> bool {
        let byte_offset = i / 8;
        self.bits[byte_offset] & (1_u8 << ((i % 8) as u8)) > 0
    }

    pub fn set(&mut self, i: usize) {
        let byte_offset = i / 8;
        self.bits[byte_offset] = self.bits[byte_offset] | (1_u8 << ((i % 8) as u8));
    }

    pub fn clear(&mut self, i: usize) {
        let byte_offset = i / 8;
        self.bits[byte_offset] = self.bits[byte_offset] ^ (1_u8 << ((i % 8) as u8));
    }
}

pub struct Array {
    pub len: i32,
    pub null_count: i32,
    pub null_bitmap: Bitmap,
    pub data: ArrayData
}

impl Array {

    pub fn new(len: usize, data: ArrayData) -> Self {
        Array { len: len as i32, data, null_bitmap: Bitmap::new(len), null_count: 0 }
    }

    pub fn data(&self) -> &ArrayData {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

}

impl From<Vec<bool>> for Array {
    fn from(v: Vec<bool>) -> Self {
        Array { len: v.len() as i32, null_count: 0, null_bitmap: Bitmap::new(v.len()), data: ArrayData::Boolean(v) }
    }
}

impl From<Vec<f32>> for Array {
    fn from(v: Vec<f32>) -> Self {
        Array { len: v.len() as i32, null_count: 0, null_bitmap: Bitmap::new(v.len()), data: ArrayData::Float32(v) }
    }
}

impl From<Vec<f64>> for Array {
    fn from(v: Vec<f64>) -> Self {
        Array { len: v.len() as i32, null_count: 0, null_bitmap: Bitmap::new(v.len()), data: ArrayData::Float64(v) }
    }
}

impl From<Vec<i32>> for Array {
    fn from(v: Vec<i32>) -> Self {
        Array { len: v.len() as i32, null_count: 0, null_bitmap: Bitmap::new(v.len()), data: ArrayData::Int32(v) }
    }
}

impl From<Vec<Option<i32>>> for Array {
    fn from(v: Vec<Option<i32>>) -> Self {
        let mut null_count = 0;
        let mut null_bitmap = Bitmap::new(v.len());
        for i in 0..v.len() {
            if v[i].is_none() {
                println!("element {} is NULL", i);
                null_count += 1;
                null_bitmap.set(i);
            }
        }
        let values : Vec<i32> = v.iter().map(|x| x.or(Some(0)).unwrap()).collect();
        Array { len: v.len() as i32, null_count, null_bitmap, data: ArrayData::Int32(values) }
    }
}

impl From<Vec<i64>> for Array {
    fn from(v: Vec<i64>) -> Self {
        Array { len: v.len() as i32, null_count: 0, null_bitmap: Bitmap::new(v.len()), data: ArrayData::Int64(v) }
    }
}

impl From<Vec<&'static str>> for Array {
    fn from(v: Vec<&'static str>) -> Self {
        Array::from(v.iter().map(|s| s.to_string()).collect::<Vec<String>>())
    }
}

impl From<Vec<String>> for Array {
    fn from(v: Vec<String>) -> Self {
        let mut offsets : Vec<i32> = Vec::with_capacity(v.len() + 1);
        let mut buf = BytesMut::with_capacity(v.len() * 32);
        offsets.push(0_i32);
        v.iter().for_each(|s| {
            buf.put(s.as_bytes());
            offsets.push(buf.len() as i32);
        });
        Array {
            len: v.len() as i32,
            null_count: 0,
            null_bitmap: Bitmap::new(v.len()),
            data: ArrayData::Utf8(ListData { offsets, bytes: buf.freeze() })
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_length() {
        assert_eq!(64, Bitmap::new(63).len());
        assert_eq!(64, Bitmap::new(64).len());
        assert_eq!(128, Bitmap::new(65).len());
    }

    #[test]
    fn test_set_bit() {
        let mut b = Bitmap::new(64);
        assert_eq!(false, b.is_set(12));
        b.set(12);
        assert_eq!(true, b.is_set(12));
    }

    #[test]
    fn test_clear_bit() {
        let mut b = Bitmap::new(64);
        assert_eq!(false, b.is_set(12));
        b.set(12);
        assert_eq!(true, b.is_set(12));
        b.clear(12);
        assert_eq!(false, b.is_set(12));
    }

    #[test]
    fn test_utf8_offsets() {
        let a = Array::from(vec!["this", "is", "a", "test"]);
        assert_eq!(4, a.len());
        match a.data() {
            &ArrayData::Utf8(ListData { ref offsets, ref bytes }) => {
                assert_eq!(11, bytes.len());
                assert_eq!(0, offsets[0]);
                assert_eq!(4, offsets[1]);
                assert_eq!(6, offsets[2]);
                assert_eq!(7, offsets[3]);
                assert_eq!(11, offsets[4]);
            },
            _ => panic!()
        }
    }

    #[test]
    fn test_utf8_slices() {
        let a = Array::from(vec!["this", "is", "a", "test"]);
        match a.data() {
            &ArrayData::Utf8(ref d) => {
                assert_eq!(4, d.len());
                assert_eq!("this", str::from_utf8(d.slice(0)).unwrap());
                assert_eq!("is", str::from_utf8(d.slice(1)).unwrap());
                assert_eq!("a", str::from_utf8(d.slice(2)).unwrap());
                assert_eq!("test", str::from_utf8(d.slice(3)).unwrap());
            },
            _ => panic!()
        }
    }

    #[test]
    fn test_optional_i32() {
        let a = Array::from(vec![Some(1), None, Some(2), Some(3), None]);
        assert_eq!(5, a.len());
        assert_eq!(false, a.null_bitmap.is_set(0));
        assert_eq!(true, a.null_bitmap.is_set(1));
        assert_eq!(false, a.null_bitmap.is_set(2));
        assert_eq!(false, a.null_bitmap.is_set(3));
        assert_eq!(true, a.null_bitmap.is_set(4));
    }
}



