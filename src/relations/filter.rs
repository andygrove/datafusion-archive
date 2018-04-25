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

use super::super::datasources::common::*;
use super::super::errors::*;
use super::super::exec::*;
use super::super::types::*;

use arrow::array::*;
use arrow::datatypes::*;

pub struct FilterRelation {
    input: Box<SimpleRelation>,
    expr: CompiledExpr,
}

impl FilterRelation {
    pub fn new(input: Box<SimpleRelation>, expr: CompiledExpr) -> Self {
        FilterRelation { input, expr }
    }
}

impl SimpleRelation for FilterRelation {
    fn scan<'a>(&'a mut self) -> Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a> {
        let filter_expr = &self.expr;
        let schema = Rc::new(self.schema().clone());

        Box::new(self.input.scan().map(move |b| {
            match b {
                Ok(ref batch) => {
                    //println!("FilterRelation batch {} rows with {} columns", batch.num_rows(), batch.num_columns());

                    assert!(batch.num_rows() > 0);
                    // evaluate the filter expression for every row in the batch
                    let x = (*filter_expr)(batch.as_ref())?;
                    match x {
                        Value::Column(ref filter_eval) => {
                            let filtered_columns: Vec<Value> = (0..batch.num_columns())
                                .map(move |column_index| {
                                    //println!("Filtering column {}", column_index);
                                    let column = batch.column(column_index);
                                    Value::Column(Rc::new(filter(column, &filter_eval)))
                                })
                                .collect();

                            let row_count_opt: Option<usize> = filtered_columns
                                .iter()
                                .map(|c| match c {
                                    Value::Scalar(_) => 1,
                                    Value::Column(ref v) => v.len(),
                                })
                                .max();

                            //TODO: should ge able to something like `row_count_opt.or_else(0)` ?
                            let row_count = match row_count_opt {
                                None => 0,
                                Some(n) => n,
                            };

                            //println!("Filtered batch has {} rows out of original {}", row_count, batch.num_rows());

                            let filtered_batch: Rc<RecordBatch> = Rc::new(DefaultRecordBatch {
                                row_count,
                                data: filtered_columns,
                                schema: schema.clone(),
                            });

                            Ok(filtered_batch)
                        }
                        Value::Scalar(_) => unimplemented!("Cannot filter on a scalar value yet"), //TODO: implement
                    }
                }
                Err(e) => Err(e),
            }
        }))
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.input.schema()
    }
}

pub fn filter(column: &Value, bools: &Array) -> Array {
    //println!("filter()");
    match column {
        &Value::Scalar(ref v) => match v.as_ref() {
//            ScalarValue::Utf8(ref s) => {
//                bools.map(|b|)
//
//            },
            ScalarValue::Null => {
                let b: Vec<i32> = vec![];
                Array::from(b)
            }
            _ => unimplemented!("unsupported scalar type for filter '{:?}'", v)
        },
        &Value::Column(ref arr) => match bools.data() {
            &ArrayData::Boolean(ref b) => match arr.as_ref().data() {
                &ArrayData::Boolean(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<bool>>(),
                ),
                &ArrayData::Float32(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<f32>>(),
                ),
                &ArrayData::Float64(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<f64>>(),
                ),
                &ArrayData::UInt8(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<u8>>(),
                ),
                &ArrayData::UInt16(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<u16>>(),
                ),
                &ArrayData::UInt32(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<u32>>(),
                ),
                &ArrayData::UInt64(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<u64>>(),
                ),
                &ArrayData::Int8(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<i8>>(),
                ),
                &ArrayData::Int16(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<i16>>(),
                ),
                &ArrayData::Int32(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<i32>>(),
                ),
                &ArrayData::Int64(ref v) => Array::from(
                    v.iter()
                        .zip(b.iter())
                        .filter(|&(_, f)| f)
                        .map(|(v, _)| v)
                        .collect::<Vec<i64>>(),
                ),
                &ArrayData::Utf8(ref v) => {
                    //println!("utf8 len = {}, bools len = {}", v.len(), b.len());
                    let mut x: Vec<String> = Vec::with_capacity(b.len() as usize);
                    for i in 0..b.len() as usize {
                        if *b.get(i) {
                            //println!("i = {}", i);
                            x.push(String::from_utf8(v.slice(i as usize).to_vec()).unwrap());
                        }
                    }
                    Array::from(x)
                }
                &ArrayData::Struct(ref _v) => unimplemented!("Cannot filter on structs yet"),
            },
            _ => panic!("Filter array expected to be boolean"),
        },
    }
}
