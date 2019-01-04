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

//! Execution of an aggregate relation

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::str;

use arrow::array::{ArrayRef, Int32Array, Float64Array, BinaryArray};
use arrow::array_ops;
use arrow::datatypes::{Field, Schema, DataType};
use arrow::record_batch::RecordBatch;

use super::error::{Result, ExecutionError};
use super::expression::{RuntimeExpr, AggregateType};
use crate::logicalplan::ScalarValue;
use super::relation::Relation;

use fnv::FnvHashMap;

pub struct AggregateRelation {
    schema: Arc<Schema>,
    input: Rc<RefCell<Relation>>,
    group_expr: Vec<RuntimeExpr>,
    aggr_expr: Vec<RuntimeExpr>,
}


impl AggregateRelation {
    pub fn new(
        schema: Arc<Schema>,
        input: Rc<RefCell<Relation>>,
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
enum GroupByScalar {
    Boolean(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Utf8(String),
}

trait AggregateFunction {
    fn accumulate(&mut self, value: &Option<ScalarValue>);
    fn result(&self) -> &Option<ScalarValue>;
    fn data_type(&self) -> &DataType;
}

struct MinFunction {
    data_type: DataType,
    value: Option<ScalarValue>,
}

impl MinFunction {
    fn new(data_type: &DataType) -> Self {
        Self { data_type: data_type.clone(), value: None }

    }
}

impl AggregateFunction for MinFunction {
    fn accumulate(&mut self, value: &Option<ScalarValue>) {
    }

    fn result(&self) -> &Option<ScalarValue> {
        &self.value
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

struct AggregateEntry {
    pub aggr_values: Vec<Rc<AggregateFunction>>
}

impl AggregateEntry {
    fn accumulate(&mut self, i: usize, value: Option<ScalarValue>) {

    }
}

/// Create an initial aggregate entry
fn create_aggregate_entry(aggr_expr: &Vec<RuntimeExpr>) -> Rc<RefCell<AggregateEntry>> {
    //println!("Creating new aggregate entry");

    let functions = aggr_expr
        .iter()
        .map(|e| match e {
            RuntimeExpr::AggregateFunction { ref f, ref t, .. } => match f {
                AggregateType::Min => Rc::new(MinFunction::new(t)) as Rc<AggregateFunction>,
//                AggregateType::Max => Box::new(MaxFunction::new(t)) as Box<AggregateFunction>,
//                AggregateType::Count => Box::new(CountFunction::new()) as Box<AggregateFunction>,
//                AggregateType::Sum => Box::new(SumFunction::new(t)) as Box<AggregateFunction>,
                _ => panic!(),
            },
            _ => panic!(),
        })
        .collect();

    Rc::new(RefCell::new(AggregateEntry {
        aggr_values: functions,
    }))
}

//TODO macros to make this code less verbose

fn array_min(array: ArrayRef, dt: &DataType) -> Result<ArrayRef> {
    match dt {
        DataType::Int32 => {
            let value = array_ops::min(array.as_any().downcast_ref::<Int32Array>().unwrap());
            Ok(Arc::new(Int32Array::from(vec![value])) as ArrayRef)
        }
        DataType::Float64 => {
            let value = array_ops::min(array.as_any().downcast_ref::<Float64Array>().unwrap());
            Ok(Arc::new(Float64Array::from(vec![value])) as ArrayRef)
        }
        //TODO support all types
        _ => Err(ExecutionError::NotImplemented("Unsupported data type for MIN".to_string()))
    }
}

fn array_max(array: ArrayRef, dt: &DataType) -> Result<ArrayRef> {
    match dt {
        DataType::Int32 => {
            let value = array_ops::max(array.as_any().downcast_ref::<Int32Array>().unwrap());
            Ok(Arc::new(Int32Array::from(vec![value])) as ArrayRef)
        }
        DataType::Float64 => {
            let value = array_ops::max(array.as_any().downcast_ref::<Float64Array>().unwrap());
            Ok(Arc::new(Float64Array::from(vec![value])) as ArrayRef)
        }
        //TODO support all types
        _ => Err(ExecutionError::NotImplemented("Unsupported data type for MAX".to_string()))
    }
}

fn update_accumulators(batch: &RecordBatch, row: usize, accumulator_set: &mut AggregateEntry, aggr_expr: &Vec<RuntimeExpr>) {
    // update the accumulators
    for j in 0..accumulator_set.aggr_values.len() {
        match &aggr_expr[j] {
            RuntimeExpr::AggregateFunction { f, args, t, .. } => {

                // evaluate argument to aggregate function
                match args[0](&batch) {
                    Ok(array) => {
                        let value: Option<ScalarValue> = match t {
                            DataType::Int32 => {
                                let z = array.as_any().downcast_ref::<Int32Array>().unwrap();
                                Some(ScalarValue::Int32(z.value(row)))
                            }
                            DataType::Float64 => {
                                let z = array.as_any().downcast_ref::<Float64Array>().unwrap();
                                Some(ScalarValue::Float64(z.value(row)))
                            }
                            _ => panic!()
                        };
                        accumulator_set.accumulate(j, value);
                    }
                    _ => panic!()
                }
            }
            _ => panic!()
        }
    }

}

impl Relation for AggregateRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.input.borrow_mut().next()? {
            Some(batch) => {

                if self.group_expr.is_empty() {

                    // perform simple aggregate on entire columns without grouping logic
                    let columns: Result<Vec<ArrayRef>> = self.aggr_expr.iter().map(|expr| match expr {
                        RuntimeExpr::AggregateFunction { f, args, t, .. } => {

                            // evaluate argument to aggregate function
                            match args[0](&batch) {
                                Ok(array) => match f {
                                    AggregateType::Min => array_min(array, &t),
                                    AggregateType::Max => array_max(array, &t),
                                    _ => Err(ExecutionError::NotImplemented("Unsupported aggregate function".to_string()))
                                }
                                Err(e) => Err(ExecutionError::ExecutionError("Failed to evaluate argument to aggregate function".to_string()))
                            }

                        },
                        _ => Err(ExecutionError::General("Invalid aggregate expression".to_string()))

                    }).collect();

                    Ok(Some(RecordBatch::new(self.schema.clone(), columns?)))

                } else {
                    let mut map: FnvHashMap<Vec<GroupByScalar>, Rc<RefCell<AggregateEntry>>> =
                        FnvHashMap::default();

                    // evaulate the group by expressions on this batch
                    let group_by_keys: Vec<ArrayRef> =
                        self.group_expr.iter()
                            .map(|e| e.get_func()(&batch))
                            .collect::<Result<Vec<ArrayRef>>>()?;


                    // iterate over each row in the batch
                    for row in 0..batch.num_rows() {

                        //NOTE: this seems pretty inefficient, performing a match and a downcast on each row

                        // create key
                        let key: Vec<GroupByScalar> = group_by_keys.iter().map(|col| {
                            //TODO: use macro to make this less verbose
                            match col.data_type() {
                                DataType::Int32 => {
                                    let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                                    GroupByScalar::Int32(array.value(row))
                                }
                                DataType::Utf8 => {
                                    let array = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                                    GroupByScalar::Utf8(String::from(str::from_utf8(array.get_value(row)).unwrap()))
                                }
                                //TODO add all types
                                _ => unimplemented!()
                            }
                        }).collect();

                        //TODO: find more elegant way to write this instead of hacking around ownership issues

                        let updated = match map.get(&key) {
                            Some(entry) => {
                                let mut accumulator_set = entry.borrow_mut();
                                update_accumulators(&batch, row, &mut accumulator_set, &self.aggr_expr);
                                true
                            }
                            None => false
                        };

                        if !updated {
                            let mut accumulator_set = create_aggregate_entry(&self.aggr_expr);
                            {
                                let mut entry_mut = accumulator_set.borrow_mut();
                                update_accumulators(&batch, row, &mut entry_mut, &self.aggr_expr);
                            }
                            map.insert(key.clone(), accumulator_set);
                        }
                    }

                    //TODO: create record batch from the accumulators

                    unimplemented!()
                }


            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

// code from original POC

///// Create an initial aggregate entry
//fn create_aggregate_entry(aggr_expr: &Vec<RuntimeExpr>) -> Rc<RefCell<AggregateEntry>> {
//    //println!("Creating new aggregate entry");
//
//    let functions = aggr_expr
//        .iter()
//        .map(|e| match e {
//            RuntimeExpr::AggregateFunction { ref f, ref t, .. } => match f {
//                AggregateType::Min => Box::new(MinFunction::new(t)) as Box<AggregateFunction>,
//                AggregateType::Max => Box::new(MaxFunction::new(t)) as Box<AggregateFunction>,
//                AggregateType::Count => Box::new(CountFunction::new()) as Box<AggregateFunction>,
//                AggregateType::Sum => Box::new(SumFunction::new(t)) as Box<AggregateFunction>,
//                _ => panic!(),
//            },
//            _ => panic!(),
//        })
//        .collect();
//
//    Rc::new(RefCell::new(AggregateEntry {
//        aggr_values: functions,
//    }))
//}
//
//macro_rules! build_aggregate_array {
//    ($TY:ty, $NAME:ident, $DATA:expr) => {{
//        let mut b: Builder<$TY> = Builder::new();
//        for v in $DATA {
//            b.push(v.$NAME().unwrap());
//        }
//        Array::from(b.finish())
//    }};
//}
//
//impl SimpleRelation for AggregateRelation {
//    fn scan<'a>(&'a mut self) -> Box<Iterator<Item = Result<Rc<RecordBatch>>> + 'a> {
//        let aggr_expr = &self.aggr_expr;
//        let group_expr = &self.group_expr;
//        //        let mut map: HashMap<Vec<GroupScalar>, Rc<RefCell<AggregateEntry>>> = HashMap::new();
//        let mut map: FnvHashMap<Vec<GroupScalar>, Rc<RefCell<AggregateEntry>>> =
//            FnvHashMap::default();
//
//        //println!("There are {} aggregate expressions", aggr_expr.len());
//
//        self.input.scan().for_each(|batch| {
//            match batch {
//                Ok(ref b) => {
//                    //println!("Processing aggregates for batch with {} rows", b.num_rows());
//
//                    // evaluate the single argument to each aggregate function
//                    let mut aggr_col_args: Vec<Vec<Value>> = Vec::with_capacity(aggr_expr.len());
//                    for i in 0..aggr_expr.len() {
//                        match aggr_expr[i] {
//                            RuntimeExpr::AggregateFunction { ref args, .. } => {
//                                // arguments to the aggregate function
//                                let aggr_func_args: Result<
//                                    Vec<Value>,
//                                > = args.iter().map(|e| (*e)(b.as_ref())).collect();
//
//                                // push the column onto the vector
//                                aggr_col_args.push(aggr_func_args.unwrap());
//                            }
//                            _ => panic!(),
//                        }
//                    }
//
//                    // evaluate the grouping expressions
//                    let group_values_result: Result<Vec<Value>> = group_expr
//                        .iter()
//                        .map(|e| e.get_func()(b.as_ref()))
//                        .collect();
//
//                    let group_values: Vec<Value> = group_values_result.unwrap(); //TODO
//
//                    if group_values.len() == 0 {
//                        // aggregate columns directly
//                        let key: Vec<GroupScalar> = Vec::with_capacity(0);
//
//                        let entry = map
//                            .entry(key)
//                            .or_insert_with(|| create_aggregate_entry(aggr_expr));
//                        let mut entry_mut = entry.borrow_mut();
//
//                        for i in 0..aggr_expr.len() {
//                            (*entry_mut).aggr_values[i]
//                                .execute(&aggr_col_args[i])
//                                .unwrap();
//                        }
//                    } else {
//                        let mut key: Vec<GroupScalar> = Vec::with_capacity(group_values.len());
//                        for _ in 0..group_values.len() {
//                            key.push(GroupScalar::Int32(0));
//                        }
//
//                        // expensive row-based aggregation by group
//                        for i in 0..b.num_rows() {
//                            write_key(&mut key, &group_values, i);
//                            //let key = make_key(&group_values, i);
//                            //println!("key = {:?}", key);
//
//                            let x = match map.get(&key) {
//                                Some(entry) => {
//                                    let mut entry_mut = entry.borrow_mut();
//
//                                    for j in 0..aggr_expr.len() {
//                                        let row_aggr_values: Vec<Value> = aggr_col_args[j].iter()
//                                            .map(|col| match col {
//                                                Value::Column(ref col) => Value::Scalar(Rc::new(get_value(col, i))),
//                                                Value::Scalar(ref v) => Value::Scalar(v.clone())
//                                            }).collect();
//                                        (*entry_mut).aggr_values[j]
//                                            .execute(&row_aggr_values)
//                                            .unwrap();
//                                    }
//
//                                    true
//                                }
//                                None => false,
//                            };
//
//                            if !x {
//                                let entry = create_aggregate_entry(aggr_expr);
//                                {
//                                    let mut entry_mut = entry.borrow_mut();
//
//                                    for j in 0..aggr_expr.len() {
//                                        let row_aggr_values: Vec<Value> = aggr_col_args[j].iter()
//                                            .map(|col| match col {
//                                                Value::Column(ref col) =>
//                                                    Value::Scalar(Rc::new(get_value(col, i))),
//                                                Value::Scalar(ref v) => Value::Scalar(v.clone())
//                                            }).collect();
//                                        (*entry_mut).aggr_values[j]
//                                            .execute(&row_aggr_values)
//                                            .unwrap();
//                                    }
//                                }
//                                map.insert(key.clone(), entry);
//                            }
//                        }
//                    }
//                }
//                Err(e) => panic!("Error aggregating batch: {:?}", e),
//            }
//        });
//
//        //        println!("Preparing results");
//
//        let mut result_columns: Vec<Vec<ScalarValue>> =
//            Vec::with_capacity(group_expr.len() + aggr_expr.len());
//        for _ in 0..group_expr.len() {
//            result_columns.push(Vec::new());
//        }
//        for _ in 0..aggr_expr.len() {
//            result_columns.push(Vec::new());
//        }
//
//        for (k, v) in map.iter() {
//            for col_index in 0..k.len() {
//                result_columns[col_index].push(k[col_index].as_scalar());
//            }
//
//            let g: Vec<Value> = v
//                .borrow()
//                .aggr_values
//                .iter()
//                .map(|v| v.finish().unwrap())
//                .collect();
//
//            //            println!("aggregate entry: {:?}", g);
//
//            for col_index in 0..g.len() {
//                result_columns[col_index + group_expr.len()].push(match g[col_index] {
//                    Value::Scalar(ref v) => v.as_ref().clone(),
//                    _ => panic!(),
//                });
//            }
//        }
//
//        let mut aggr_batch = DefaultRecordBatch {
//            schema: Rc::new(Schema::empty()),
//            data: Vec::new(),
//            row_count: map.len(),
//        };
//
//        // create Arrow arrays from grouping scalar values
//        for i in 0..group_expr.len() {
//            //TODO: should not use string version of group keys
//            let mut tmp: Vec<String> = vec![];
//            for v in &result_columns[i] {
//                tmp.push(format!("{}", v));
//            }
//            aggr_batch
//                .data
//                .push(Value::Column(Rc::new(Array::from(tmp))));
//        }
//
//        // create Arrow arrays from aggregate scalar values
//        for i in 0..aggr_expr.len() {
//            match aggr_expr[i] {
//                RuntimeExpr::AggregateFunction { ref t, .. } => {
//                    let aggr_values = &result_columns[i + group_expr.len()];
//
//                    let array: Array = match t {
//                        DataType::Boolean => build_aggregate_array!(bool, get_bool, aggr_values),
//                        DataType::UInt8 => build_aggregate_array!(u8, get_u8, aggr_values),
//                        DataType::UInt16 => build_aggregate_array!(u16, get_u16, aggr_values),
//                        DataType::UInt32 => build_aggregate_array!(u32, get_u32, aggr_values),
//                        DataType::UInt64 => build_aggregate_array!(u64, get_u64, aggr_values),
//                        DataType::Int8 => build_aggregate_array!(i8, get_i8, aggr_values),
//                        DataType::Int16 => build_aggregate_array!(i16, get_i16, aggr_values),
//                        DataType::Int32 => build_aggregate_array!(i32, get_i32, aggr_values),
//                        DataType::Int64 => build_aggregate_array!(i64, get_i64, aggr_values),
//                        DataType::Float32 => build_aggregate_array!(f32, get_f32, aggr_values),
//                        DataType::Float64 => build_aggregate_array!(f64, get_f64, aggr_values),
//                        DataType::Utf8 => {
//                            let mut b: ListBuilder<u8> =
//                                ListBuilder::with_capacity(aggr_values.len());
//                            for v in aggr_values {
//                                b.push(v.get_string().unwrap().as_bytes());
//                            }
//                            Array::new(
//                                aggr_values.len(),
//                                ArrayData::Utf8(ListArray::from(b.finish())),
//                            )
//                        }
//                        _ => unimplemented!("No support for aggregate with return type {:?}", t),
//                    };
//
//                    aggr_batch.data.push(Value::Column(Rc::new(array)))
//                }
//                _ => panic!(),
//            }
//        }
//
//        let tb: Rc<RecordBatch> = Rc::new(aggr_batch);
//
//        // create iterator over the single batch
//        let v = vec![Ok(tb)];
//        Box::new(v.into_iter())
//    }
//
//    fn schema<'a>(&'a self) -> &'a Schema {
//        self.schema.as_ref()
//    }
//}


#[cfg(test)]
mod tests {
    use super::super::super::logicalplan::Expr;
    use super::super::context::ExecutionContext;
    use super::super::datasource::CsvDataSource;
    use super::super::expression;
    use super::super::relation::DataSourceRelation;
    use super::*;
    use arrow::csv;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::fs::File;

    #[test]
    fn min_lat() {
        let schema = schema();
        let relation = load_cities();
        let context = ExecutionContext::new();

        let aggr_expr =
            vec![expression::compile_expr(&context, &Expr::AggregateFunction {
                name: String::from("min"),
                args: vec![Expr::Column(1)],
                return_type: DataType::Float64,
            }, &schema).unwrap()];

        let aggr_schema = Arc::new(Schema::new(vec![
            Field::new("min_lat", DataType::Float64, false),
        ]));

        let mut projection = AggregateRelation::new(aggr_schema,relation, vec![], aggr_expr);
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(1, batch.num_columns());
        let min_lat = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(50.376289, min_lat.value(0));
    }

    #[test]
    fn max_lat() {
        let schema = schema();
        let relation = load_cities();
        let context = ExecutionContext::new();

        let aggr_expr =
            vec![expression::compile_expr(&context, &Expr::AggregateFunction {
                name: String::from("max"),
                args: vec![Expr::Column(1)],
                return_type: DataType::Float64,
            }, &schema).unwrap()];

        let aggr_schema = Arc::new(Schema::new(vec![
            Field::new("max_lat", DataType::Float64, false),
        ]));

        let mut projection = AggregateRelation::new(aggr_schema,relation, vec![], aggr_expr);
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(1, batch.num_columns());
        let max_lat = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(57.477772, max_lat.value(0));
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]))
    }

    fn load_cities() -> Rc<RefCell<Relation>> {
        let schema = schema();
        let file = File::open("test/data/uk_cities.csv").unwrap();
        let arrow_csv_reader = csv::Reader::new(file, schema.clone(), true, 1024, None);
        let ds = CsvDataSource::new(schema.clone(), arrow_csv_reader);
        Rc::new(RefCell::new(DataSourceRelation::new(Rc::new(
            RefCell::new(ds),
        ))))
    }

}
