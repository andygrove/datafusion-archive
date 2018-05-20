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

//! ndjson (newline-delimited JSON) Support

use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::rc::Rc;

//use arrow::array::ListArray;
//use arrow::bitmap::*;
//use arrow::builder::*;
use arrow::datatypes::Schema;
//use arrow::list_builder::ListBuilder;

use super::super::errors::*;
use super::super::types::*;
use super::common::*;

pub struct NdJsonFile {
    lines: Box<Iterator<Item=io::Result<String>>>
}

impl NdJsonFile {

    pub fn open(filename: &str) -> Self {
        //let lines = file.lines();
        let f = File::open(filename).unwrap();
        let reader = BufReader::new(f);
        let it = reader.lines();
        NdJsonFile { lines: Box::new(it) }
    }
}

impl DataSource for NdJsonFile {

    fn schema(&self) -> &Rc<Schema> {
        unimplemented!()
    }

    fn next(&mut self) -> Option<Result<Rc<RecordBatch>>> {
        unimplemented!()
    }
}

