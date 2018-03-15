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

use std::io::{BufWriter, Write};
use std::net::TcpStream;

use super::data::*;

extern crate byteorder;

use self::byteorder::{WriteBytesExt, LittleEndian};

fn write_column(col: &ColumnData) {

    let mut stream = BufWriter::new(TcpStream::connect("127.0.0.1:34254").unwrap());

    use std::slice;
    use std::mem;

    //stream.write_u32(col.len() as u32).unwrap();
    let slice_u8: &[u8] = match col {
        &ColumnData::Float(ref v) => {
            let slice : &[f32] = &v;
            unsafe {
                slice::from_raw_parts(
                    slice.as_ptr() as *const u8,
                    slice.len() * mem::size_of::<f32>(),
                )
            }
        },
        &ColumnData::Double(ref v) => {
            let slice : &[f64] = &v;
            unsafe {
                slice::from_raw_parts(
                    slice.as_ptr() as *const u8,
                    slice.len() * mem::size_of::<f64>(),
                )
            }
        },
        _ => unimplemented!()
    };

    let mut wtr = vec![];
    wtr.write_u32::<LittleEndian>(col.len() as u32).unwrap();
    stream.write(&wtr).unwrap();
    stream.write(slice_u8).unwrap();


}

