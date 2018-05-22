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

//! Aggregate / Grouping Relation

use std::cell::RefCell;
use std::rc::Rc;
use std::str;

use super::super::datasources::common::*;
use super::super::errors::*;
use super::super::exec::*;
use super::super::functions::count::CountFunction;
use super::super::functions::max::MaxFunction;
use super::super::functions::min::MinFunction;
use super::super::functions::sum::SumFunction;
use super::super::types::*;

use arrow::array::ListArray;
use arrow::builder::*;
use arrow::datatypes::*;
use arrow::list_builder::*;

use fnv::FnvHashMap;

pub struct AggregateRelation {
    schema: Rc<Schema>,
    input: Box<SimpleRelation>,
    group_expr: Vec<RuntimeExpr>,
    aggr_expr: Vec<RuntimeExpr>,
}

struct AggregateEntry {
    aggr_values: Vec<Box<AggregateFunction>>,
}

impl AggregateRelation {
    pub fn new(
        schema: Rc<Schema>,
        input: Box<SimpleRelation>,
        group_expr: Vec<RuntimeExpr>,
        aggr_expr: Vec<RuntimeExpr>,
    ) -> Self {
        AggregateRelation {
            schema,
            input,
            group_expr,
            aggr_expr,
        }
    }
}

/// Enumeration of types that can be used in a GROUP BY expression
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum GroupScalar {
    Boolean(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Utf8(Rc<String>),
}

impl GroupScalar {
    fn as_scalar(&self) -> ScalarValue {
        match *self {
            GroupScalar::Boolean(v) => ScalarValue::Boolean(v),
            GroupScalar::UInt8(v) => ScalarValue::UInt8(v),
            GroupScalar::UInt16(v) => ScalarValue::UInt16(v),
            GroupScalar::UInt32(v) => ScalarValue::UInt32(v),
            GroupScalar::UInt64(v) => ScalarValue::UInt64(v),
            GroupScalar::Int8(v) => ScalarValue::Int8(v),
            GroupScalar::Int16(v) => ScalarValue::Int16(v),
            GroupScalar::Int32(v) => ScalarValue::Int32(v),
            GroupScalar::Int64(v) => ScalarValue::Int64(v),
            GroupScalar::Utf8(ref v) => ScalarValue::Utf8(v.clone()),
        }
    }
}

/// Make a hash map key from a list of values
fn write_key(key: &mut Vec<GroupScalar>, group_values: &Vec<Value>, i: usize) {
    for j in 0..group_values.len() {
        key[j] = match group_values[j] {
            Value::Scalar(ref vv) => match vv.as_ref() {
                ScalarValue::Boolean(x) => GroupScalar::Boolean(*x),
                _ => unimplemented!(),
            },
            Value::Column(ref array) => match array.data() {
                ArrayData::Boolean(ref buf) => GroupScalar::Boolean(*buf.get(i)),
                ArrayData::Int8(ref buf) => GroupScalar::Int8(*buf.get(i)),
                ArrayData::Int16(ref buf) => GroupScalar::Int16(*buf.get(i)),
                ArrayData::Int32(ref buf) => GroupScalar::Int32(*buf.get(i)),
                ArrayData::Int64(ref buf) => GroupScalar::Int64(*buf.get(i)),
                ArrayData::UInt8(ref buf) => GroupScalar::UInt8(*buf.get(i)),
                ArrayData::UInt16(ref buf) => GroupScalar::UInt16(*buf.get(i)),
                ArrayData::UInt32(ref buf) => GroupScalar::UInt32(*buf.get(i)),
                ArrayData::UInt64(ref buf) => GroupScalar::UInt64(*buf.get(i)),
                ArrayData::Utf8(ref list) => {
                    GroupScalar::Utf8(Rc::new(str::from_utf8(list.get(i)).unwrap().to_string()))
                }
                _ => unimplemented!("Unsupported datatype for aggregate grouping expression"),
            },
        };
    }
}

/// Create an initial aggregate entry
fn create_aggregate_entry(aggr_expr: &Vec<RuntimeExpr>) -> Rc<RefCell<AggregateEntry>> {
    //println!("Creating new aggregate entry");

    let functions = aggr_expr
        .iter()
        .map(|e| match e {
            RuntimeExpr::AggregateFunction { ref f, ref t, .. } => match f {
                AggregateType::Min => Box::new(MinFunction::new(t)) as Box<AggregateFunction>,
                AggregateType::Max => Box::new(MaxFunction::new(t)) as Box<AggregateFunction>,
                AggregateType::Count => Box::new(CountFunction::new()) as Box<AggregateFunction>,
                AggregateType::Sum => Box::new(SumFunction::new(t)) as Box<AggregateFunction>,
                _ => panic!(),
            },
            _ => panic!(),
        })
        .collect();

    Rc::new(RefCell::new(AggregateEntry {
        aggr_values: functions,
    }))
}

macro_rules! build_aggregate_array {
    ($TY:ty, $NAME:ident, $DATA:expr) => {{
        let mut b: Builder<$TY> = Builder::new();
        for v in $DATA {
            b.push(v.$NAME().unwrap());
        }
        Array::from(b.finish())
    }};
}

impl SimpleRelation for AggregateRelation {
    fn scan<'a>(&'a mut self) -> Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a> {
        let aggr_expr = &self.aggr_expr;
        let group_expr = &self.group_expr;
        //        let mut map: HashMap<Vec<GroupScalar>, Rc<RefCell<AggregateEntry>>> = HashMap::new();
        let mut map: FnvHashMap<Vec<GroupScalar>, Rc<RefCell<AggregateEntry>>> =
            FnvHashMap::default();

        //println!("There are {} aggregate expressions", aggr_expr.len());

        self.input.scan().for_each(|batch| {
            match batch {
                Ok(ref b) => {
                    //println!("Processing aggregates for batch with {} rows", b.num_rows());

                    // evaluate the single argument to each aggregate function
                    let mut aggr_col_args: Vec<Vec<Value>> = Vec::with_capacity(aggr_expr.len());
                    for i in 0..aggr_expr.len() {
                        match aggr_expr[i] {
                            RuntimeExpr::AggregateFunction { ref args, .. } => {
                                // arguments to the aggregate function
                                let aggr_func_args: Result<
                                    Vec<Value>,
                                > = args.iter().map(|e| (*e)(b.as_ref())).collect();

                                // push the column onto the vector
                                aggr_col_args.push(aggr_func_args.unwrap());
                            }
                            _ => panic!(),
                        }
                    }

                    // evaluate the grouping expressions
                    let group_values_result: Result<Vec<Value>> = group_expr
                        .iter()
                        .map(|e| e.get_func()(b.as_ref()))
                        .collect();

                    let group_values: Vec<Value> = group_values_result.unwrap(); //TODO

                    if group_values.len() == 0 {
                        // aggregate columns directly
                        let key: Vec<GroupScalar> = Vec::with_capacity(0);

                        let entry = map.entry(key)
                            .or_insert_with(|| create_aggregate_entry(aggr_expr));
                        let mut entry_mut = entry.borrow_mut();

                        for i in 0..aggr_expr.len() {
                            (*entry_mut).aggr_values[i]
                                .execute(&aggr_col_args[i])
                                .unwrap();
                        }
                    } else {
                        let mut key: Vec<GroupScalar> = Vec::with_capacity(group_values.len());
                        for _ in 0..group_values.len() {
                            key.push(GroupScalar::Int32(0));
                        }

                        // expensive row-based aggregation by group
                        for i in 0..b.num_rows() {
                            write_key(&mut key, &group_values, i);
                            //let key = make_key(&group_values, i);
                            //println!("key = {:?}", key);

                            let x = match map.get(&key) {
                                Some(entry) => {
                                    let mut entry_mut = entry.borrow_mut();

                                    for j in 0..aggr_expr.len() {
                                        let row_aggr_values: Vec<Value> = aggr_col_args[j].iter()
                                            .map(|col| match col {
                                                Value::Column(ref col) => Value::Scalar(Rc::new(get_value(col, i))),
                                                Value::Scalar(ref v) => Value::Scalar(v.clone())
                                            }).collect();
                                        (*entry_mut).aggr_values[j]
                                            .execute(&row_aggr_values)
                                            .unwrap();
                                    }

                                    true
                                }
                                None => false,
                            };

                            if !x {
                                let entry = create_aggregate_entry(aggr_expr);
                                {
                                    let mut entry_mut = entry.borrow_mut();

                                    for j in 0..aggr_expr.len() {
                                        let row_aggr_values: Vec<Value> = aggr_col_args[j].iter()
                                            .map(|col| match col {
                                                Value::Column(ref col) =>
                                                    Value::Scalar(Rc::new(get_value(col, i))),
                                                Value::Scalar(ref v) => Value::Scalar(v.clone())
                                            }).collect();
                                        (*entry_mut).aggr_values[j]
                                            .execute(&row_aggr_values)
                                            .unwrap();
                                    }
                                }
                                map.insert(key.clone(), entry);
                            }
                        }
                    }
                }
                Err(e) => panic!("Error aggregating batch: {:?}", e),
            }
        });

        //        println!("Preparing results");

        let mut result_columns: Vec<Vec<ScalarValue>> =
            Vec::with_capacity(group_expr.len() + aggr_expr.len());
        for _ in 0..group_expr.len() {
            result_columns.push(Vec::new());
        }
        for _ in 0..aggr_expr.len() {
            result_columns.push(Vec::new());
        }

        for (k, v) in map.iter() {
            for col_index in 0..k.len() {
                result_columns[col_index].push(k[col_index].as_scalar());
            }

            let g: Vec<Value> = v.borrow()
                .aggr_values
                .iter()
                .map(|v| v.finish().unwrap())
                .collect();

            //            println!("aggregate entry: {:?}", g);

            for col_index in 0..g.len() {
                result_columns[col_index + group_expr.len()].push(match g[col_index] {
                    Value::Scalar(ref v) => v.as_ref().clone(),
                    _ => panic!(),
                });
            }
        }

        let mut aggr_batch = DefaultRecordBatch {
            schema: Rc::new(Schema::empty()),
            data: Vec::new(),
            row_count: map.len(),
        };

        // create Arrow arrays from grouping scalar values
        for i in 0..group_expr.len() {
            //TODO: should not use string version of group keys
            let mut tmp: Vec<String> = vec![];
            for v in &result_columns[i] {
                tmp.push(format!("{}", v));
            }
            aggr_batch
                .data
                .push(Value::Column(Rc::new(Array::from(tmp))));
        }

        // create Arrow arrays from aggregate scalar values
        for i in 0..aggr_expr.len() {
            match aggr_expr[i] {
                RuntimeExpr::AggregateFunction { ref t, .. } => {
                    let aggr_values = &result_columns[i + group_expr.len()];

                    let array: Array = match t {
                        DataType::Boolean => build_aggregate_array!(bool, get_bool, aggr_values),
                        DataType::UInt8 => build_aggregate_array!(u8, get_u8, aggr_values),
                        DataType::UInt16 => build_aggregate_array!(u16, get_u16, aggr_values),
                        DataType::UInt32 => build_aggregate_array!(u32, get_u32, aggr_values),
                        DataType::UInt64 => build_aggregate_array!(u64, get_u64, aggr_values),
                        DataType::Int8 => build_aggregate_array!(i8, get_i8, aggr_values),
                        DataType::Int16 => build_aggregate_array!(i16, get_i16, aggr_values),
                        DataType::Int32 => build_aggregate_array!(i32, get_i32, aggr_values),
                        DataType::Int64 => build_aggregate_array!(i64, get_i64, aggr_values),
                        DataType::Float32 => build_aggregate_array!(f32, get_f32, aggr_values),
                        DataType::Float64 => build_aggregate_array!(f64, get_f64, aggr_values),
                        DataType::Utf8 => {
                            let mut b: ListBuilder<u8> =
                                ListBuilder::with_capacity(aggr_values.len());
                            for v in aggr_values {
                                b.push(v.get_string().unwrap().as_bytes());
                            }
                            Array::new(aggr_values.len(), ArrayData::Utf8(ListArray::from(b.finish())))
                        }
                        _ => unimplemented!("No support for aggregate with return type {:?}", t),
                    };

                    aggr_batch.data.push(Value::Column(Rc::new(array)))
                }
                _ => panic!(),
            }
        }

        let tb: Rc<RecordBatch> = Rc::new(aggr_batch);

        // create iterator over the single batch
        let v = vec![Ok(tb)];
        Box::new(v.into_iter())
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        self.schema.as_ref()
    }
}
