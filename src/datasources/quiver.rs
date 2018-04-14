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

use arrow::array::{Array, ArrayData};
use arrow::builder::Builder;
use arrow::datatypes::{DataType, Field, Schema};
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
struct QuiverWriter<W: Write> {
    w: BufWriter<W>,
}

impl<W> QuiverWriter<W>
where
    W: Write,
{
    fn write_header(&mut self, schema: &Schema) -> Result<()> {
        //TODO: write magic bytes + file format version in fixed-size header record

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

    fn write_row_group(&mut self, batch: Vec<Rc<Array>>) -> Result<()> {
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
                    mem::transmute::<&[i32], &[u8]>(list.offsets.slice(0, a.len as usize))
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
struct QuiverReader<R: Read> {
    r: BufReader<R>,
}

impl<R> QuiverReader<R>
where
    R: Read,
{
    pub fn read_array(&mut self) -> Result<Rc<Array>> {
        let len = self.r.read_i32::<LittleEndian>()? as usize;
        println!("len: {}", len);
        let type_id = self.r.read_u8()?;
        println!("type: {}", type_id);
        if type_id == TYPE_ID_UINT16 {
            println!("reading TYPE_ID_UINT16");
            let mut builder: Builder<u16> = Builder::with_capacity(len);
            builder.set_len(len);
            self.r.read(unsafe {
                mem::transmute::<&mut [u16], &mut [u8]>(builder.slice_mut(0, len))
            })?;
            Ok(Rc::new(Array::from(builder.finish())))
        } else {
            panic!()
        }
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
}
