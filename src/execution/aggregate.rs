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

//! Execution of a simple aggregate relation containing MIN, MAX, COUNT, SUM aggregate functions
//! with optional GROUP BY columns

use std::cell::RefCell;
use std::rc::Rc;
use std::str;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, Float64Array, Int32Array};
use arrow::array_ops;
use arrow::builder::{ArrayBuilder, Float64Builder, Int16Builder, Int32Builder, ListArrayBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::error::{ExecutionError, Result};
use super::expression::{AggregateType, RuntimeExpr};
use super::relation::Relation;
use crate::logicalplan::ScalarValue;

use fnv::FnvHashMap;

/// An aggregate relation is made up of zero or more grouping expressions and one
/// or more aggregate expressions
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

/// Enumeration of types that can be used in a GROUP BY expression (all primitives except for
/// floating point numerics)
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

/// Common trait for all aggregation functions
trait AggregateFunction {
    /// Get the function name (used for debugging)
    fn name(&self) -> &str;
    fn accumulate_scalar(&mut self, value: &Option<ScalarValue>);
    fn result(&self) -> &Option<ScalarValue>;
    fn data_type(&self) -> &DataType;
}

#[derive(Debug)]
struct MinFunction {
    data_type: DataType,
    value: Option<ScalarValue>,
}

impl MinFunction {
    fn new(data_type: &DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            value: None,
        }
    }
}

impl AggregateFunction for MinFunction {
    fn name(&self) -> &str {
        "min"
    }

    fn accumulate_scalar(&mut self, value: &Option<ScalarValue>) {
        if self.value.is_none() {
            self.value = value.clone();
        } else if value.is_some() {
            self.value = match (&self.value, value) {
                (Some(ScalarValue::UInt8(a)), Some(ScalarValue::UInt8(b))) => {
                    Some(ScalarValue::UInt8(*a.min(b)))
                }
                (Some(ScalarValue::UInt16(a)), Some(ScalarValue::UInt16(b))) => {
                    Some(ScalarValue::UInt16(*a.min(b)))
                }
                (Some(ScalarValue::UInt32(a)), Some(ScalarValue::UInt32(b))) => {
                    Some(ScalarValue::UInt32(*a.min(b)))
                }
                (Some(ScalarValue::UInt64(a)), Some(ScalarValue::UInt64(b))) => {
                    Some(ScalarValue::UInt64(*a.min(b)))
                }
                (Some(ScalarValue::Int8(a)), Some(ScalarValue::Int8(b))) => {
                    Some(ScalarValue::Int8(*a.min(b)))
                }
                (Some(ScalarValue::Int16(a)), Some(ScalarValue::Int16(b))) => {
                    Some(ScalarValue::Int16(*a.min(b)))
                }
                (Some(ScalarValue::Int32(a)), Some(ScalarValue::Int32(b))) => {
                    Some(ScalarValue::Int32(*a.min(b)))
                }
                (Some(ScalarValue::Int64(a)), Some(ScalarValue::Int64(b))) => {
                    Some(ScalarValue::Int64(*a.min(b)))
                }
                (Some(ScalarValue::Float32(a)), Some(ScalarValue::Float32(b))) => {
                    Some(ScalarValue::Float32(a.min(*b)))
                }
                (Some(ScalarValue::Float64(a)), Some(ScalarValue::Float64(b))) => {
                    Some(ScalarValue::Float64(a.min(*b)))
                }
                _ => panic!("unsupported data type for MIN"),
            }
        }
    }

    fn result(&self) -> &Option<ScalarValue> {
        &self.value
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[derive(Debug)]
struct MaxFunction {
    data_type: DataType,
    value: Option<ScalarValue>,
}

impl MaxFunction {
    fn new(data_type: &DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            value: None,
        }
    }
}

impl AggregateFunction for MaxFunction {
    fn name(&self) -> &str {
        "max"
    }

    fn accumulate_scalar(&mut self, value: &Option<ScalarValue>) {
        if self.value.is_none() {
            self.value = value.clone();
        } else if value.is_some() {
            self.value = match (&self.value, value) {
                (Some(ScalarValue::UInt8(a)), Some(ScalarValue::UInt8(b))) => {
                    Some(ScalarValue::UInt8(*a.max(b)))
                }
                (Some(ScalarValue::UInt16(a)), Some(ScalarValue::UInt16(b))) => {
                    Some(ScalarValue::UInt16(*a.max(b)))
                }
                (Some(ScalarValue::UInt32(a)), Some(ScalarValue::UInt32(b))) => {
                    Some(ScalarValue::UInt32(*a.max(b)))
                }
                (Some(ScalarValue::UInt64(a)), Some(ScalarValue::UInt64(b))) => {
                    Some(ScalarValue::UInt64(*a.max(b)))
                }
                (Some(ScalarValue::Int8(a)), Some(ScalarValue::Int8(b))) => {
                    Some(ScalarValue::Int8(*a.max(b)))
                }
                (Some(ScalarValue::Int16(a)), Some(ScalarValue::Int16(b))) => {
                    Some(ScalarValue::Int16(*a.max(b)))
                }
                (Some(ScalarValue::Int32(a)), Some(ScalarValue::Int32(b))) => {
                    Some(ScalarValue::Int32(*a.max(b)))
                }
                (Some(ScalarValue::Int64(a)), Some(ScalarValue::Int64(b))) => {
                    Some(ScalarValue::Int64(*a.max(b)))
                }
                (Some(ScalarValue::Float32(a)), Some(ScalarValue::Float32(b))) => {
                    Some(ScalarValue::Float32(a.max(*b)))
                }
                (Some(ScalarValue::Float64(a)), Some(ScalarValue::Float64(b))) => {
                    Some(ScalarValue::Float64(a.max(*b)))
                }
                _ => panic!("unsupported data type for MAX"),
            }
        }
    }

    fn result(&self) -> &Option<ScalarValue> {
        &self.value
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

struct AccumulatorSet {
    aggr_values: Vec<Rc<RefCell<AggregateFunction>>>,
}

impl AccumulatorSet {
    fn accumulate_scalar(&mut self, i: usize, value: Option<ScalarValue>) {
        let mut accumulator = self.aggr_values[i].borrow_mut();
        let before = accumulator.result().clone();
        accumulator.accumulate_scalar(&value);
        let after = accumulator.result().clone();
        println!(
            "{} accumulate_scalar {:?} changed value from {:?} to {:?}",
            accumulator.name(),
            value,
            before,
            after
        );
    }

    fn values(&self) -> Vec<Option<ScalarValue>> {
        self.aggr_values
            .iter()
            .map(|x| x.borrow().result().clone())
            .collect()
    }
}

#[derive(Debug)]
struct MapEntry {
    k: Vec<GroupByScalar>,
    v: Vec<Option<ScalarValue>>,
}

/// Create an initial aggregate entry
fn create_accumulators(aggr_expr: &Vec<RuntimeExpr>) -> AccumulatorSet {
    let functions = aggr_expr
        .iter()
        .map(|e| match e {
            RuntimeExpr::AggregateFunction { ref f, ref t, .. } => match f {
                AggregateType::Min => {
                    Rc::new(RefCell::new(MinFunction::new(t))) as Rc<RefCell<AggregateFunction>>
                }
                AggregateType::Max => {
                    Rc::new(RefCell::new(MaxFunction::new(t))) as Rc<RefCell<AggregateFunction>>
                }
                _ => panic!("unsupported aggregate function"),
            },
            _ => panic!("invalid aggregate expression"),
        })
        .collect();

    AccumulatorSet {
        aggr_values: functions,
    }
}

//TODO macros to make this code less verbose

fn array_min(array: ArrayRef, dt: &DataType) -> Result<Option<ScalarValue>> {
    match dt {
        //        DataType::Int32 => {
        //            let value = array_ops::min(array.as_any().downcast_ref::<Int32Array>().unwrap());
        //            Ok(Arc::new(Int32Array::from(vec![value])) as ArrayRef)
        //        }
        DataType::Float64 => {
            match array_ops::min(array.as_any().downcast_ref::<Float64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Float64(n))),
                None => Ok(None),
            }
        }
        //TODO support all types
        _ => Err(ExecutionError::NotImplemented(
            "Unsupported data type for MIN".to_string(),
        )),
    }
}

fn array_max(array: ArrayRef, dt: &DataType) -> Result<Option<ScalarValue>> {
    match dt {
        //        DataType::Int32 => {
        //            let value = array_ops::max(array.as_any().downcast_ref::<Int32Array>().unwrap());
        //            Ok(Arc::new(Int32Array::from(vec![value])) as ArrayRef)
        //        }
        DataType::Float64 => {
            match array_ops::max(array.as_any().downcast_ref::<Float64Array>().unwrap()) {
                Some(n) => Ok(Some(ScalarValue::Float64(n))),
                None => Ok(None),
            }
        }
        //TODO support all types
        _ => Err(ExecutionError::NotImplemented(
            "Unsupported data type for MAX".to_string(),
        )),
    }
}

fn update_accumulators(
    batch: &RecordBatch,
    row: usize,
    accumulator_set: &mut AccumulatorSet,
    aggr_expr: &Vec<RuntimeExpr>,
) {
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
                            _ => panic!(),
                        };
                        accumulator_set.accumulate_scalar(j, value);
                    }
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }
    }
}

impl Relation for AggregateRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.group_expr.is_empty() {
            self.without_group_by()
        } else {
            self.with_group_by()
        }
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

impl AggregateRelation {
    /// perform simple aggregate on entire columns without grouping logic
    fn without_group_by(&mut self) -> Result<Option<RecordBatch>> {
        let aggr_expr_count = self.aggr_expr.len();
        let mut accumulator_set = create_accumulators(&self.aggr_expr);

        while let Some(batch) = self.input.borrow_mut().next()? {
            for i in 0..aggr_expr_count {
                match &self.aggr_expr[i] {
                    RuntimeExpr::AggregateFunction { f, args, t, .. } => {
                        // evaluate argument to aggregate function
                        match args[0](&batch) {
                            Ok(array) => match f {
                                AggregateType::Min => {
                                    accumulator_set.accumulate_scalar(i, array_min(array, &t)?)
                                }
                                AggregateType::Max => {
                                    accumulator_set.accumulate_scalar(i, array_max(array, &t)?)
                                }
                                _ => {
                                    return Err(ExecutionError::NotImplemented(
                                        "Unsupported aggregate function".to_string(),
                                    ))
                                }
                            },
                            Err(e) => {
                                return Err(ExecutionError::ExecutionError(
                                    "Failed to evaluate argument to aggregate function".to_string(),
                                ))
                            }
                        }
                    }
                    _ => {
                        return Err(ExecutionError::General(
                            "Invalid aggregate expression".to_string(),
                        ))
                    }
                }
            }
        }

        let mut result_columns: Vec<ArrayRef> = vec![];

        for i in 0..aggr_expr_count {
            let mut accum = accumulator_set.aggr_values[i].borrow();
            match accum.data_type() {
                DataType::Int32 => {
                    let b = Int32Builder::new(1);
                    result_columns.push(Arc::new(b.finish()));
                }
                DataType::Float64 => {
                    let mut b = Float64Builder::new(1);
                    match accum.result() {
                        Some(ScalarValue::Float64(n)) => b.push(*n)?,
                        Some(_) => panic!(),
                        None => b.push_null()?,
                    };
                    result_columns.push(Arc::new(b.finish()));
                }
                _ => unimplemented!(),
            }
        }

        Ok(Some(RecordBatch::new(self.schema.clone(), result_columns)))
    }

    fn with_group_by(&mut self) -> Result<Option<RecordBatch>> {
        //NOTE this whole method is currently very inefficient with too many per-row operations
        // involving pattern matching and downcasting ... I'm sure this can be re-implemented in
        // a much more efficient way that takes better advantage of Arrow

        // create map to store aggregate results
        let mut map: FnvHashMap<Vec<GroupByScalar>, Rc<RefCell<AccumulatorSet>>> =
            FnvHashMap::default();

        while let Some(batch) = self.input.borrow_mut().next()? {
            // evaulate the group by expressions on this batch
            let group_by_keys: Vec<ArrayRef> = self
                .group_expr
                .iter()
                .map(|e| e.get_func()(&batch))
                .collect::<Result<Vec<ArrayRef>>>()?;

            // iterate over each row in the batch
            for row in 0..batch.num_rows() {
                // create key
                let key: Vec<GroupByScalar> = group_by_keys
                    .iter()
                    .map(|col| {
                        //TODO: use macro to make this less verbose
                        match col.data_type() {
                            DataType::Int32 => {
                                let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                                GroupByScalar::Int32(array.value(row))
                            }
                            DataType::Utf8 => {
                                let array = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                                GroupByScalar::Utf8(String::from(
                                    str::from_utf8(array.get_value(row)).unwrap(),
                                ))
                            }
                            //TODO add all types
                            _ => unimplemented!(),
                        }
                    })
                    .collect();

                //TODO: find more elegant way to write this instead of hacking around ownership issues

                let updated = match map.get(&key) {
                    Some(entry) => {
                        let mut accumulator_set = entry.borrow_mut();
                        update_accumulators(&batch, row, &mut accumulator_set, &self.aggr_expr);
                        true
                    }
                    None => false,
                };

                if !updated {
                    let accumulator_set =
                        Rc::new(RefCell::new(create_accumulators(&self.aggr_expr)));
                    {
                        let mut entry_mut = accumulator_set.borrow_mut();
                        update_accumulators(&batch, row, &mut entry_mut, &self.aggr_expr);
                    }
                    map.insert(key.clone(), accumulator_set);
                }
            }
        }

        // convert the map to a vec to make it easier to build arrays
        let entries: Vec<MapEntry> = map
            .iter()
            .map(|(k, v)| {
                let x = v.borrow();
                MapEntry {
                    k: k.clone(),
                    v: x.values(),
                }
            })
            .collect();

        // build the result arrays
        let mut result_arrays: Vec<ArrayRef> =
            Vec::with_capacity(self.group_expr.len() + self.aggr_expr.len());

        // grouping values
        for i in 0..self.group_expr.len() {
            match self.group_expr[i].get_type() {
                //TOD: macros, all types
                DataType::Int16 => {
                    let mut builder = Int16Builder::new(entries.len());
                    for j in 0..entries.len() {
                        match entries[j].k[i] {
                            GroupByScalar::Int16(n) => builder.push(n).unwrap(),
                            _ => panic!(),
                        }
                    }
                    result_arrays.push(Arc::new(builder.finish()));
                }
                DataType::Int32 => {
                    let mut builder = Int32Builder::new(entries.len());
                    for j in 0..entries.len() {
                        match entries[j].k[i] {
                            GroupByScalar::Int32(n) => builder.push(n).unwrap(),
                            _ => panic!(),
                        }
                    }
                    result_arrays.push(Arc::new(builder.finish()));
                }
                _ => unimplemented!(),
            }
        }

        // aggregate values
        for i in 0..self.aggr_expr.len() {
            match self.aggr_expr[i].get_type() {
                //TOD: macros, all types
                DataType::Int16 => {
                    let mut builder = Int16Builder::new(entries.len());
                    for j in 0..entries.len() {
                        match entries[j].v[i] {
                            Some(ScalarValue::Int16(n)) => builder.push(n).unwrap(),
                            None => builder.push_null().unwrap(),
                            _ => panic!(),
                        }
                    }
                    result_arrays.push(Arc::new(builder.finish()));
                }
                DataType::Int32 => {
                    let mut builder = Int32Builder::new(entries.len());
                    for j in 0..entries.len() {
                        match entries[j].v[i] {
                            Some(ScalarValue::Int32(n)) => builder.push(n).unwrap(),
                            None => builder.push_null().unwrap(),
                            _ => panic!(),
                        }
                    }
                    result_arrays.push(Arc::new(builder.finish()));
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::new(entries.len());
                    for j in 0..entries.len() {
                        match entries[j].v[i] {
                            Some(ScalarValue::Float64(n)) => builder.push(n).unwrap(),
                            None => builder.push_null().unwrap(),
                            _ => panic!(),
                        }
                    }
                    result_arrays.push(Arc::new(builder.finish()));
                }
                _ => unimplemented!(),
            }
        }

        Ok(Some(RecordBatch::new(self.schema.clone(), result_arrays)))
    }
}

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
        let schema = uk_cities_schema();
        let relation = load_csv("test/data/uk_cities.csv", &schema);
        let context = ExecutionContext::new();

        let aggr_expr = vec![expression::compile_expr(
            &context,
            &Expr::AggregateFunction {
                name: String::from("min"),
                args: vec![Expr::Column(1)],
                return_type: DataType::Float64,
            },
            &schema,
        )
        .unwrap()];

        let aggr_schema = Arc::new(Schema::new(vec![Field::new(
            "min_lat",
            DataType::Float64,
            false,
        )]));

        let mut projection = AggregateRelation::new(aggr_schema, relation, vec![], aggr_expr);
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(1, batch.num_columns());
        let min_lat = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(50.376289, min_lat.value(0));
    }

    #[test]
    fn max_lat() {
        let schema = uk_cities_schema();
        let relation = load_csv("test/data/uk_cities.csv", &schema);
        let context = ExecutionContext::new();

        let aggr_expr = vec![expression::compile_expr(
            &context,
            &Expr::AggregateFunction {
                name: String::from("max"),
                args: vec![Expr::Column(1)],
                return_type: DataType::Float64,
            },
            &schema,
        )
        .unwrap()];

        let aggr_schema = Arc::new(Schema::new(vec![Field::new(
            "max_lat",
            DataType::Float64,
            false,
        )]));

        let mut projection = AggregateRelation::new(aggr_schema, relation, vec![], aggr_expr);
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(1, batch.num_columns());
        let max_lat = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(57.477772, max_lat.value(0));
    }

    #[test]
    fn test_min_max_group_by() {
        let schema = aggr_test_schema();
        let relation = load_csv("test/data/aggregate_test_1.csv", &schema);

        let context = ExecutionContext::new();

        let group_by_expr = expression::compile_expr(&context, &Expr::Column(0), &schema).unwrap();

        let min_expr = expression::compile_expr(
            &context,
            &Expr::AggregateFunction {
                name: String::from("min"),
                args: vec![Expr::Column(1)],
                return_type: DataType::Float64,
            },
            &schema,
        )
        .unwrap();

        let max_expr = expression::compile_expr(
            &context,
            &Expr::AggregateFunction {
                name: String::from("max"),
                args: vec![Expr::Column(1)],
                return_type: DataType::Float64,
            },
            &schema,
        )
        .unwrap();

        let aggr_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("min", DataType::Float64, false),
            Field::new("max", DataType::Float64, false),
        ]));

        let mut projection = AggregateRelation::new(
            aggr_schema,
            relation,
            vec![group_by_expr],
            vec![min_expr, max_expr],
        );
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(3, batch.num_columns());
        assert_eq!(3, batch.num_rows());

        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let min = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let max = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(3, a.value(0));
        assert_eq!(1.0, min.value(0));
        assert_eq!(2.0, max.value(0));

        assert_eq!(1, a.value(1));
        assert_eq!(1.1, min.value(1));
        assert_eq!(2.2, max.value(1));

        assert_eq!(2, a.value(2));
        assert_eq!(3.3, min.value(2));
        assert_eq!(5.5, max.value(2));
    }

    fn uk_cities_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]))
    }

    fn aggr_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float64, false),
        ]))
    }

    fn load_csv(filename: &str, schema: &Arc<Schema>) -> Rc<RefCell<Relation>> {
        let file = File::open(filename).unwrap();
        let arrow_csv_reader = csv::Reader::new(file, schema.clone(), true, 1024, None);
        let ds = CsvDataSource::new(schema.clone(), arrow_csv_reader);
        Rc::new(RefCell::new(DataSourceRelation::new(Rc::new(
            RefCell::new(ds),
        ))))
    }

}
