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

use std::rc::Rc;
use std::io::Error;
use std::fs::File;
use std::io::{BufReader, Read};

use arrow::datatypes::*;
use arrow::array::*;
use arrow::builder::*;
use arrow::buffer::*;

use csv;
use csv::*;
use parquet::file::reader::*;
use parquet::column::reader::*;

trait RecordBatch {
    fn schema(&self) -> Rc<Schema>;
    fn num_columns(&self) -> usize;
    fn num_rows(&self) -> usize;
    fn column(&self, index: usize) -> &Rc<Array>;
}

struct DefaultRecordBatch {
    row_count: usize,
    data: Vec<Rc<Array>>
}

impl RecordBatch for DefaultRecordBatch {

    fn schema(&self) -> Rc<Schema> {
        unimplemented!()
    }

    fn num_columns(&self) -> usize {
        self.data.len()
    }

    fn num_rows(&self) -> usize {
        self.row_count
    }

    fn column(&self, index: usize) -> &Rc<Array> {
        &self.data[index]
    }
}


trait DataSource {
    fn next(&self) -> Option<Box<RecordBatch>>;
}

pub struct CsvFile {
    reader: Reader<BufReader<File>>

}

impl CsvFile {
    fn open(file: File) -> Self {
        let buf_reader = BufReader::with_capacity(8 * 1024 * 1024, file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        CsvFile { reader: csv_reader }
    }
}

impl DataSource for CsvFile {

    fn next(&self) -> Option<Box<RecordBatch>> {
        Some(Box::new(DefaultRecordBatch { data: vec![], row_count: 0 }) as Box<RecordBatch>)
    }
}

pub struct ParquetFile {
    reader: SerializedFileReader
}

impl ParquetFile {

    fn open(file: File) -> Self {
        ParquetFile { reader: SerializedFileReader::new(file).unwrap() }
    }
}
//
//pub struct ParquetFileIter {
//    index: usize
//}
//
//
//impl Iterator for ParquetFileIter {
//    type Item = Box<RecordBatch>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        unimplemented!()
//    }
//}

impl DataSource for ParquetFile {
    fn next(&self) -> Option<Box<RecordBatch>> {
        unimplemented!()
    }


//    fn next(&self) -> Result<Option<Box<RecordBatch>>, Error> {
//
//        //TODO: error handling
//
//        let row_group_reader = self.reader.get_row_group(self.index).unwrap();
//
//        let batch_size = 1024;
//
//        let mut arrays: Vec<Rc<Array>> = Vec::with_capacity(row_group_reader.num_columns());
//        let mut row_count = 0;
//
//        for i in 0..row_group_reader.num_columns() {
//
//            let array: Option<Array> = match row_group_reader.get_column_reader(i) {
//
//                //TODO: support all column types
//
//                Ok(ColumnReader::Int32ColumnReader(ref mut r)) => {
//                    let mut builder : Builder<i32> = Builder::with_capacity(batch_size);
//                    match r.read_batch(batch_size, None, None, unsafe {builder.slice_mut(0, batch_size)}) {
//                        Ok((count,_)) => {
//                            row_count = count;
//                            builder.set_len(count);
//                            Some(Array::from(builder.finish()))
//                        },
//                        _ => panic!("error")
//                    }
//                }
//                _ => {
//                    println!("column type not supported");
//                    None
//                }
//            };
//
//            if let Some(a) = array {
//                arrays.push(Rc::new(a));
//            }
//
//        }
//
//        Ok(Some(Box::new(DefaultRecordBatch { data: arrays, row_count })))
//    }
//    fn iter(&self) -> Box<Iterator<Item=Box<RecordBatch>>> {
//        unimplemented!()
//    }
//    fn iter(&self) -> Box<Iterator<Item=Box<RecordBatch>>> {
//        unimplemented!()
//    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_csv() {
        let file = File::open("test/data/uk_cities.csv").unwrap();
        let csv = CsvFile::open(file);
//        let batch = csv.next().unwrap().unwrap();
//        println!("rows: {}; cols: {}", batch.num_rows(), batch.num_columns());
    }

    #[test]
    fn test_parquet() {
        let file = File::open("test/data/alltypes_plain.parquet").unwrap();
        let parquet = ParquetFile::open(file);
//        let batch = parquet.next().unwrap().unwrap();
//        println!("rows: {}; cols: {}", batch.num_rows(), batch.num_columns());
    }
}
