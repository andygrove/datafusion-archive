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

//! Quiver is a native file format for storing Arrow data. It is similar conceptually
//! to the Apache Parquet file format but is much simpler and optimized for Arrow data.

use std::io::{BufReader, BufWriter, Read, Result, Write};
use std::mem;
use std::rc::Rc;
use std::str;

use arrow::array::{Array, ArrayData};
use arrow::buffer::Buffer;
use arrow::builder::Builder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::list::List;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

const TYPE_ID_BOOL: u8 = 1;
const TYPE_ID_UINT8: u8 = 2;
const TYPE_ID_UINT16: u8 = 3;
const TYPE_ID_UINT32: u8 = 4;
const TYPE_ID_UINT64: u8 = 5;
const TYPE_ID_INT8: u8 = 6;
const TYPE_ID_INT16: u8 = 7;
const TYPE_ID_INT32: u8 = 8;
const TYPE_ID_INT64: u8 = 9;
const TYPE_ID_FLOAT32: u8 = 10;
const TYPE_ID_FLOAT64: u8 = 11;
const TYPE_ID_UTF8: u8 = 12;

const TYPE_ID_STRUCT: u8 = 20;

/// Quiver file writer
pub struct QuiverWriter<W: Write> {
    w: BufWriter<W>,
}

impl<W> QuiverWriter<W>
where
    W: Write,
{
    pub fn new(w: W) -> Self {
        QuiverWriter { w: BufWriter::new(w) }
    }

    pub fn write_schema(&mut self, schema: &Schema) -> Result<()> {
        // write number of fields
        self.w
            .write_i32::<LittleEndian>(schema.columns.len() as i32)?;
        // write field descriptors
        for field in &schema.columns {
            self.write_field(field)?;
        }

        Ok(())
    }

    fn write_field(&mut self, field: &Field) -> Result<()> {
        // write field name
        self.w.write_i32::<LittleEndian>(field.name.len() as i32)?;
        self.w.write_all(field.name.as_bytes())?;
        // write data type
        match field.data_type {
            DataType::Boolean => self.w.write_u8(TYPE_ID_BOOL)?,
            DataType::UInt8 => self.w.write_u8(TYPE_ID_UINT8)?,
            DataType::UInt16 => self.w.write_u8(TYPE_ID_UINT16)?,
            DataType::UInt32 => self.w.write_u8(TYPE_ID_UINT32)?,
            DataType::UInt64 => self.w.write_u8(TYPE_ID_UINT64)?,
            DataType::Int8 => self.w.write_u8(TYPE_ID_INT8)?,
            DataType::Int16 => self.w.write_u8(TYPE_ID_INT16)?,
            DataType::Int32 => self.w.write_u8(TYPE_ID_INT32)?,
            DataType::Int64 => self.w.write_u8(TYPE_ID_INT64)?,
            DataType::Float16 => panic!("Float16 is not supported yet"),
            DataType::Float32 => self.w.write_u8(TYPE_ID_FLOAT32)?,
            DataType::Float64 => self.w.write_u8(TYPE_ID_FLOAT64)?,
            DataType::Utf8 => self.w.write_u8(TYPE_ID_UTF8)?,
            DataType::Struct(ref fields) => {
                self.w.write_u8(TYPE_ID_STRUCT)?;
                // write number of fields
                self.w.write_i32::<LittleEndian>(fields.len() as i32)?;
                // write field descriptors
                for field in fields.iter() {
                    self.write_field(&field)?;
                }
            }
        }

        Ok(())
    }

    pub fn write_row_group(&mut self, batch: Vec<Rc<Array>>) -> Result<()> {
        // write array count as i32
        self.w.write_i32::<LittleEndian>(batch.len() as i32)?;
        for array in &batch {
            self.write_array(array.as_ref())?;
        }
        Ok(())
    }

    fn write_array(&mut self, a: &Array) -> Result<()> {
        // write array length as i32
        self.w.write_i32::<LittleEndian>(a.len)?;
        // write the data
        match a.data() {
            ArrayData::Boolean(buf) => {
                self.w.write_u8(TYPE_ID_BOOL)?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[bool], &[u8]>(buf.slice(0, a.len as usize))
                })
            }
            ArrayData::UInt8(buf) => {
                self.w.write_u8(TYPE_ID_UINT8)?;
                self.w.write_all(buf.slice(0, a.len as usize))
            }
            ArrayData::UInt16(buf) => {
                self.w.write_u8(TYPE_ID_UINT16)?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[u16], &[u8]>(buf.slice(0, a.len as usize))
                })
            }
            ArrayData::UInt32(buf) => {
                self.w.write_u8(TYPE_ID_UINT32)?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[u32], &[u8]>(buf.slice(0, a.len as usize))
                })
            }
            ArrayData::UInt64(buf) => {
                self.w.write_u8(TYPE_ID_UINT64)?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[u64], &[u8]>(buf.slice(0, a.len as usize))
                })
            }
            ArrayData::Int8(buf) => {
                self.w.write_u8(TYPE_ID_INT8)?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[i8], &[u8]>(buf.slice(0, a.len as usize))
                })
            }
            ArrayData::Int16(buf) => {
                self.w.write_u8(TYPE_ID_INT16)?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[i16], &[u8]>(buf.slice(0, a.len as usize))
                })
            }
            ArrayData::Int32(buf) => {
                self.w.write_u8(TYPE_ID_INT32)?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[i32], &[u8]>(buf.slice(0, a.len as usize))
                })
            }
            ArrayData::Int64(buf) => {
                self.w.write_u8(TYPE_ID_INT64)?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[i64], &[u8]>(buf.slice(0, a.len as usize))
                })
            }
            ArrayData::Float32(buf) => {
                self.w.write_u8(TYPE_ID_FLOAT32)?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[f32], &[u8]>(buf.slice(0, a.len as usize))
                })
            }
            ArrayData::Float64(buf) => {
                self.w.write_u8(TYPE_ID_FLOAT64)?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[f64], &[u8]>(buf.slice(0, a.len as usize))
                })
            }
            ArrayData::Utf8(ref list) => {
                self.w.write_u8(TYPE_ID_UTF8)?;
                self.w.write_i32::<LittleEndian>(list.offsets.len())?;
                self.w.write_all(unsafe {
                    mem::transmute::<&[i32], &[u8]>(list.offsets.slice(0, list.offsets.len() as usize))
                })?;
                self.w.write_i32::<LittleEndian>(list.data.len())?;
                self.w.write_all(list.data.slice(0, a.len as usize))
            }
            ArrayData::Struct(ref list) => {
                self.w.write_u8(TYPE_ID_STRUCT)?;
                // number of arrays (a little redundant perhaps)
                self.w.write_i32::<LittleEndian>(list.len() as i32)?;
                for array in list.iter() {
                    self.write_array(array.as_ref())?;
                }
                Ok(())
            }
        }
    }
}

/// Quiver file reader
pub struct QuiverReader<R: Read> {
    r: BufReader<R>,
}

macro_rules! read_primitive_buffer {
    ($SELF:ident, $TY:ty, $LEN:expr) => {
        {
            let mut builder: Builder<$TY> = Builder::with_capacity($LEN);
            builder.set_len($LEN);
            $SELF.r.read(unsafe {
                mem::transmute::<&mut [$TY], &mut [u8]>(builder.slice_mut(0, $LEN))
            })?;
            builder.finish()
        }
    }
}

impl<R> QuiverReader<R>
where
    R: Read,
{
    pub fn new(r: R) -> Self {
        QuiverReader { r: BufReader::new(r) }
    }

    pub fn read_schema(&mut self) -> Result<Rc<Schema>> {
        let field_count = self.r.read_i32::<LittleEndian>()?;
        let mut fields: Vec<Field> = vec![];
        for i in 0..field_count {
            println!("Reading field {}", i);
            fields.push(self.read_field()?.as_ref().clone());
        }
        Ok(Rc::new(Schema::new(fields)))
    }

    /// Read meta-data for a single field
    fn read_field(&mut self) -> Result<Rc<Field>> {

        // read name
        let name_len = self.r.read_i32::<LittleEndian>()? as usize;
        println!("field name length: {}", name_len);
        let mut name_buf: Vec<u8> = Vec::with_capacity(name_len);
        unsafe { name_buf.set_len(name_len) };
        let buf = name_buf.as_mut();
        self.r.read_exact(buf)?;
        let name_str = str::from_utf8_mut(buf).unwrap();

        // read datatype byte
        let type_id = self.r.read_u8()?;

        println!("field name: {}, type: {}", name_str, type_id);

        // convert to Arrow DataType
        let dt = if type_id == TYPE_ID_BOOL { DataType::Boolean }
        else if type_id == TYPE_ID_UINT8 { DataType::UInt8 }
        else if type_id == TYPE_ID_UINT16 { DataType::UInt16 }
        else if type_id == TYPE_ID_UINT32 { DataType::UInt32 }
        else if type_id == TYPE_ID_UINT64 { DataType::UInt64 }
        else if type_id == TYPE_ID_INT8 { DataType::Int8 }
        else if type_id == TYPE_ID_INT16 { DataType::Int16 }
        else if type_id == TYPE_ID_INT32 { DataType::Int32 }
        else if type_id == TYPE_ID_INT64 { DataType::Int64 }
        else if type_id == TYPE_ID_FLOAT32 { DataType::Float32 }
        else if type_id == TYPE_ID_FLOAT64 { DataType::Float64 }
        else if type_id == TYPE_ID_UTF8 { DataType::Utf8 }
        else if type_id == TYPE_ID_STRUCT {
            // read number of fields
            let field_count = self.r.read_i32::<LittleEndian>()? as usize;
            let mut fields: Vec<Field> = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                fields.push(self.read_field()?.as_ref().clone());
            }
            DataType::Struct(fields)
        }
        else { panic!("invlid datatype type_id in field meta-data") };

        Ok(Rc::new(Field::new(name_str, dt, true)))
    }

    pub fn read_row_group(&mut self) -> Result<Vec<Rc<Array>>> {
        let count = self.r.read_i32::<LittleEndian>()? as usize;
        let mut arrays: Vec<Rc<Array>> = Vec::with_capacity(count);
        for i in 0..count {
            println!("reading array {}", i);
            arrays.push(self.read_array()?);
        }
        Ok(arrays)
    }

    pub fn read_array(&mut self) -> Result<Rc<Array>> {
        let len = self.r.read_i32::<LittleEndian>()? as usize;
        println!("array len: {}", len);
        let type_id = self.r.read_u8()?;
        println!("array type: {}", type_id);
        if type_id == TYPE_ID_BOOL { Ok(Rc::new(Array::from(read_primitive_buffer!(self, bool, len)))) }
        else if type_id == TYPE_ID_UINT8 { Ok(Rc::new(Array::from(read_primitive_buffer!(self, u8, len)))) }
        else if type_id == TYPE_ID_UINT16 { Ok(Rc::new(Array::from(read_primitive_buffer!(self, u16, len)))) }
        else if type_id == TYPE_ID_UINT32 { Ok(Rc::new(Array::from(read_primitive_buffer!(self, u32, len)))) }
        else if type_id == TYPE_ID_UINT64 { Ok(Rc::new(Array::from(read_primitive_buffer!(self, u64, len)))) }
        else if type_id == TYPE_ID_INT8 { Ok(Rc::new(Array::from(read_primitive_buffer!(self, i8, len)))) }
        else if type_id == TYPE_ID_INT16 { Ok(Rc::new(Array::from(read_primitive_buffer!(self, i16, len)))) }
        else if type_id == TYPE_ID_INT32 { Ok(Rc::new(Array::from(read_primitive_buffer!(self, i32, len)))) }
        else if type_id == TYPE_ID_INT64 { Ok(Rc::new(Array::from(read_primitive_buffer!(self, i64, len)))) }
        else if type_id == TYPE_ID_FLOAT32 { Ok(Rc::new(Array::from(read_primitive_buffer!(self, f32, len)))) }
        else if type_id == TYPE_ID_FLOAT64 { Ok(Rc::new(Array::from(read_primitive_buffer!(self, f64, len)))) }
        else if type_id == TYPE_ID_UTF8 {
            println!("Reading UTF8");
            let offsets_count = self.r.read_i32::<LittleEndian>()? as usize;
            let offsets = read_primitive_buffer!(self, i32, offsets_count);
            let data_count = self.r.read_i32::<LittleEndian>()? as usize;
            let data = read_primitive_buffer!(self, u8, data_count);
            Ok(Rc::new(Array {
                len: (offsets_count-1) as i32,
                data: ArrayData::Utf8(List { data, offsets }),
                null_count: 0,
                validity_bitmap: None
            }))
        }
        else if type_id == TYPE_ID_STRUCT {
            println!("Reading STRUCT");
            let array_count = self.r.read_i32::<LittleEndian>()? as usize;
            let mut arrays: Vec<Rc<Array>> = Vec::with_capacity(array_count);
            for _ in 0..array_count {
                arrays.push(self.read_array()?);
            }
            Ok(Rc::new(Array::from(arrays)))
        }
        else { panic!("invalid type_id when reading array") }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::prelude::*;

    #[test]
    fn test_array() {
        let v: Vec<u16> = vec![1, 2, 3, 4, 5];
        let array = Array::from(v);

        // write the file
        {
            let mut file = File::create("array_u16.quiver").unwrap();
            let mut w = QuiverWriter {
                w: BufWriter::new((file)),
            };
            w.write_array(&array).unwrap();
        }

        // read the file
        let file = File::open("array_u16.quiver").unwrap();
        let mut r = QuiverReader {
            r: BufReader::new((file)),
        };
        let array2 = r.read_array().unwrap();
        assert_eq!(5, array2.len());
    }

    #[test]
    fn write_read_file() {
        // define schema for data source (csv file)
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let names: Rc<Array> = Rc::new(Array::from(vec!["Elgin, Scotland, the UK".to_string(),
                                                        "Stoke-on-Trent, Staffordshire, the UK".to_string()]));

        let lats: Rc<Array> = Rc::new(Array::from(vec![57.653484, 53.002666]));
        let lngs: Rc<Array> = Rc::new(Array::from(vec![-3.335724, -2.179404]));

        // write the quiver file
        {
            let file = File::create("_uk_cities.quiver").unwrap();
            let mut w = QuiverWriter::new(file);
            w.write_schema(&schema).unwrap();
            w.write_row_group(vec![names, lats, lngs]).unwrap();
        }

        // read the quiver file
        let file = File::open("_uk_cities.quiver").unwrap();
        let mut r = QuiverReader::new(file);
        let schema = r.read_schema().unwrap();
        assert_eq!(3, schema.columns.len());

        let data = r.read_row_group().unwrap();
        assert_eq!(3, data.len());

    }
}
