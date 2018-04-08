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

use std::cell::RefCell;
use std::clone::Clone;
use std::collections::HashMap;
use std::convert::*;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufWriter, Error};
use std::iter::Iterator;
use std::rc::Rc;
use std::str;
use std::string::String;

use arrow::array::*;
use arrow::datatypes::*;

//use futures::{Future, Stream};
//use hyper::Client;
//use tokio_core::reactor::Core;
//use hyper::{Method, Request};
//use hyper::header::{ContentLength, ContentType};

use super::datasource::*;
use super::logical::*;
use super::sqlast::ASTNode::*;
use super::sqlcompiler::*;
use super::sqlparser::*;
use super::types::*;
//use super::cluster::*;

#[derive(Debug, Clone)]
pub enum DFConfig {
    Local,
    Remote { etcd: String },
}

#[derive(Debug)]
pub enum DataFrameError {
    IoError(Error),
    ExecError(ExecutionError),
    InvalidColumn(String),
    NotImplemented,
}

impl From<ExecutionError> for DataFrameError {
    fn from(e: ExecutionError) -> Self {
        DataFrameError::ExecError(e)
    }
}

impl From<Error> for DataFrameError {
    fn from(e: Error) -> Self {
        DataFrameError::IoError(e)
    }
}

/// DataFrame is an abstraction of a logical plan and a schema
pub trait DataFrame {
    /// Projection
    fn select(&self, expr: Vec<Expr>) -> Result<Rc<DataFrame>, DataFrameError>;

    /// Selection
    fn filter(&self, expr: Expr) -> Result<Rc<DataFrame>, DataFrameError>;

    /// Sorting
    fn sort(&self, expr: Vec<Expr>) -> Result<Rc<DataFrame>, DataFrameError>;

    /// Return an expression representing the specified column
    fn col(&self, column_name: &str) -> Result<Expr, DataFrameError>;

    fn schema(&self) -> &Rc<Schema>;

    fn plan(&self) -> &Rc<LogicalPlan>;

    /// show N rows (useful for debugging)
    fn show(&self, count: usize);

    fn create_execution_plan(&self) -> Result<Box<SimpleRelation>, ExecutionError>;
}

macro_rules! compare_arrays_inner {
    ($V1:ident, $V2:ident, $F:expr) => {
        match ($V1.data(), $V2.data()) {
            (&ArrayData::Float32(ref a), &ArrayData::Float32(ref b)) =>
                Ok(a.iter().zip(b.iter()).map($F).collect::<Vec<bool>>()),
            (&ArrayData::Float64(ref a), &ArrayData::Float64(ref b)) =>
                Ok(a.iter().zip(b.iter()).map($F).collect::<Vec<bool>>()),
            (&ArrayData::Int8(ref a), &ArrayData::Int8(ref b)) =>
                Ok(a.iter().zip(b.iter()).map($F).collect::<Vec<bool>>()),
            (&ArrayData::Int16(ref a), &ArrayData::Int16(ref b)) =>
                Ok(a.iter().zip(b.iter()).map($F).collect::<Vec<bool>>()),
            (&ArrayData::Int32(ref a), &ArrayData::Int32(ref b)) =>
                Ok(a.iter().zip(b.iter()).map($F).collect::<Vec<bool>>()),
            (&ArrayData::Int64(ref a), &ArrayData::Int64(ref b)) =>
                Ok(a.iter().zip(b.iter()).map($F).collect::<Vec<bool>>()),
            //(&ArrayData::Utf8(ref a), &ScalarValue::Utf8(ref b)) => a.iter().map(|n| n > b).collect(),
            _ => Err(ExecutionError::Custom("Unsupported types in compare_arrays_inner".to_string()))
        }
    }
}

macro_rules! compare_arrays {
    ($V1:ident, $V2:ident, $F:expr) => {
        Ok(Rc::new(Value::Column(Rc::new(Array::from(
            compare_arrays_inner!($V1, $V2, $F)?,
        )))))
    };
}

macro_rules! compare_array_with_scalar_inner {
    ($V1:ident, $V2:ident, $F:expr) => {
        match ($V1.data(), $V2.as_ref()) {
            (&ArrayData::Float32(ref a), &ScalarValue::Float32(b)) => {
                Ok(a.iter().map(|aa| (aa, b)).map($F).collect::<Vec<bool>>())
            }
            (&ArrayData::Float64(ref a), &ScalarValue::Float64(b)) => {
                Ok(a.iter().map(|aa| (aa, b)).map($F).collect::<Vec<bool>>())
            }
            _ => Err(ExecutionError::Custom(
                "Unsupported types in compare_array_with_scalar_inner".to_string(),
            )),
        }
    };
}

macro_rules! compare_array_with_scalar {
    ($V1:ident, $V2:ident, $F:expr) => {
        Ok(Rc::new(Value::Column(Rc::new(Array::from(
            compare_array_with_scalar_inner!($V1, $V2, $F)?,
        )))))
    };
}

impl Value {
    pub fn eq(&self, other: &Value) -> Result<Rc<Value>, ExecutionError> {
        match (self, other) {
            (&Value::Column(ref v1), &Value::Column(ref v2)) => {
                compare_arrays!(v1, v2, |(aa, bb)| aa == bb)
            }
            (&Value::Column(ref v1), &Value::Scalar(ref v2)) => {
                compare_array_with_scalar!(v1, v2, |(aa, bb)| aa == bb)
            }
            (&Value::Scalar(ref v1), &Value::Column(ref v2)) => {
                compare_array_with_scalar!(v2, v1, |(aa, bb)| aa == bb)
            }
            (&Value::Scalar(ref _v1), &Value::Scalar(ref _v2)) => unimplemented!(),
        }
    }

    pub fn not_eq(&self, other: &Value) -> Result<Rc<Value>, ExecutionError> {
        match (self, other) {
            (&Value::Column(ref v1), &Value::Column(ref v2)) => {
                compare_arrays!(v1, v2, |(aa, bb)| aa != bb)
            }
            (&Value::Column(ref v1), &Value::Scalar(ref v2)) => {
                compare_array_with_scalar!(v1, v2, |(aa, bb)| aa != bb)
            }
            (&Value::Scalar(ref v1), &Value::Column(ref v2)) => {
                compare_array_with_scalar!(v2, v1, |(aa, bb)| aa != bb)
            }
            (&Value::Scalar(ref _v1), &Value::Scalar(ref _v2)) => unimplemented!(),
        }
    }

    pub fn lt(&self, other: &Value) -> Result<Rc<Value>, ExecutionError> {
        match (self, other) {
            (&Value::Column(ref v1), &Value::Column(ref v2)) => {
                compare_arrays!(v1, v2, |(aa, bb)| aa < bb)
            }
            (&Value::Column(ref v1), &Value::Scalar(ref v2)) => {
                compare_array_with_scalar!(v1, v2, |(aa, bb)| aa < bb)
            }
            (&Value::Scalar(ref v1), &Value::Column(ref v2)) => {
                compare_array_with_scalar!(v2, v1, |(aa, bb)| aa < bb)
            }
            (&Value::Scalar(ref _v1), &Value::Scalar(ref _v2)) => unimplemented!(),
        }
    }

    pub fn lt_eq(&self, other: &Value) -> Result<Rc<Value>, ExecutionError> {
        match (self, other) {
            (&Value::Column(ref v1), &Value::Column(ref v2)) => {
                compare_arrays!(v1, v2, |(aa, bb)| aa <= bb)
            }
            (&Value::Column(ref v1), &Value::Scalar(ref v2)) => {
                compare_array_with_scalar!(v1, v2, |(aa, bb)| aa <= bb)
            }
            (&Value::Scalar(ref v1), &Value::Column(ref v2)) => {
                compare_array_with_scalar!(v2, v1, |(aa, bb)| aa <= bb)
            }
            (&Value::Scalar(ref _v1), &Value::Scalar(ref _v2)) => unimplemented!(),
        }
    }

    pub fn gt(&self, other: &Value) -> Result<Rc<Value>, ExecutionError> {
        match (self, other) {
            (&Value::Column(ref v1), &Value::Column(ref v2)) => {
                compare_arrays!(v1, v2, |(aa, bb)| aa >= bb)
            }
            (&Value::Column(ref v1), &Value::Scalar(ref v2)) => {
                compare_array_with_scalar!(v1, v2, |(aa, bb)| aa >= bb)
            }
            (&Value::Scalar(ref v1), &Value::Column(ref v2)) => {
                compare_array_with_scalar!(v2, v1, |(aa, bb)| aa >= bb)
            }
            (&Value::Scalar(ref _v1), &Value::Scalar(ref _v2)) => unimplemented!(),
        }
    }

    pub fn gt_eq(&self, other: &Value) -> Result<Rc<Value>, ExecutionError> {
        match (self, other) {
            (&Value::Column(ref v1), &Value::Column(ref v2)) => {
                compare_arrays!(v1, v2, |(aa, bb)| aa > bb)
            }
            (&Value::Column(ref v1), &Value::Scalar(ref v2)) => {
                compare_array_with_scalar!(v1, v2, |(aa, bb)| aa > bb)
            }
            (&Value::Scalar(ref v1), &Value::Column(ref v2)) => {
                compare_array_with_scalar!(v2, v1, |(aa, bb)| aa > bb)
            }
            (&Value::Scalar(ref _v1), &Value::Scalar(ref _v2)) => unimplemented!(),
        }
    }

    pub fn add(&self, _other: &Value) -> Result<Rc<Value>, ExecutionError> {
        unimplemented!()
    }
    pub fn subtract(&self, _other: &Value) -> Result<Rc<Value>, ExecutionError> {
        unimplemented!()
    }
    pub fn divide(&self, _other: &Value) -> Result<Rc<Value>, ExecutionError> {
        unimplemented!()
    }
    pub fn multiply(&self, _other: &Value) -> Result<Rc<Value>, ExecutionError> {
        unimplemented!()
    }
}

/// Compiled Expression (basically just a closure to evaluate the expression at runtime)
pub type CompiledExpr = Box<Fn(&RecordBatch) -> Result<Rc<Value>, ExecutionError>>;

/// Compiles a relational expression into a closure
pub fn compile_expr(ctx: &ExecutionContext, expr: &Expr) -> Result<CompiledExpr, ExecutionError> {
    match expr {
        &Expr::Literal(ref lit) => {
            let literal_value = lit.clone();
            Ok(Box::new(move |_| {
                // literal values are a bit special - we don't repeat them in a vector
                // because it would be redundant, so we have a single value in a vector instead
                Ok(Rc::new(Value::Scalar(Rc::new(literal_value.clone()))))
            }))
        }
        &Expr::Column(index) => Ok(Box::new(move |batch: &RecordBatch| {
            Ok(Rc::new((*batch.column(index)).clone()))
        })),
        &Expr::BinaryExpr {
            ref left,
            ref op,
            ref right,
        } => {
            let left_expr = compile_expr(ctx, left)?;
            let right_expr = compile_expr(ctx, right)?;
            match op {
                &Operator::Eq => Ok(Box::new(move |batch: &RecordBatch| {
                    let left_values = left_expr(batch)?;
                    let right_values = right_expr(batch)?;
                    left_values.eq(&right_values)
                })),
                &Operator::NotEq => Ok(Box::new(move |batch: &RecordBatch| {
                    let left_values = left_expr(batch)?;
                    let right_values = right_expr(batch)?;
                    left_values.not_eq(&right_values)
                })),
                &Operator::Lt => Ok(Box::new(move |batch: &RecordBatch| {
                    let left_values = left_expr(batch)?;
                    let right_values = right_expr(batch)?;
                    left_values.lt(&right_values)
                })),
                &Operator::LtEq => Ok(Box::new(move |batch: &RecordBatch| {
                    let left_values = left_expr(batch)?;
                    let right_values = right_expr(batch)?;
                    left_values.lt_eq(&right_values)
                })),
                &Operator::Gt => Ok(Box::new(move |batch: &RecordBatch| {
                    let left_values = left_expr(batch)?;
                    let right_values = right_expr(batch)?;
                    left_values.gt(&right_values)
                })),
                &Operator::GtEq => Ok(Box::new(move |batch: &RecordBatch| {
                    let left_values = left_expr(batch)?;
                    let right_values = right_expr(batch)?;
                    left_values.gt_eq(&right_values)
                })),
                &Operator::Plus => Ok(Box::new(move |batch: &RecordBatch| {
                    let left_values = left_expr(batch)?;
                    let right_values = right_expr(batch)?;
                    left_values.add(&right_values)
                })),
                &Operator::Minus => Ok(Box::new(move |batch: &RecordBatch| {
                    let left_values = left_expr(batch)?;
                    let right_values = right_expr(batch)?;
                    left_values.subtract(&right_values)
                })),
                &Operator::Divide => Ok(Box::new(move |batch: &RecordBatch| {
                    let left_values = left_expr(batch)?;
                    let right_values = right_expr(batch)?;
                    left_values.divide(&right_values)
                })),
                &Operator::Multiply => Ok(Box::new(move |batch: &RecordBatch| {
                    let left_values = left_expr(batch)?;
                    let right_values = right_expr(batch)?;
                    left_values.multiply(&right_values)
                })),
                _ => {
                    return Err(ExecutionError::Custom(format!(
                        "Unsupported binary operator '{:?}'",
                        op
                    )))
                }
            }
        }
        &Expr::Sort { ref expr, .. } => {
            //NOTE sort order is ignored here and is handled during sort execution
            compile_expr(ctx, expr)
        }
        &Expr::ScalarFunction { ref name, ref args } => {
            ////println!("Executing function {}", name);

            // evaluate the arguments to the function
            let compiled_args: Result<Vec<CompiledExpr>, ExecutionError> =
                args.iter().map(|e| compile_expr(ctx, e)).collect();

            let compiled_args_ok = compiled_args?;

            let func = ctx.load_function_impl(name.as_ref())?;

            Ok(Box::new(move |batch| {
                let arg_values: Result<Vec<Rc<Value>>, ExecutionError> =
                    compiled_args_ok.iter().map(|expr| expr(batch)).collect();

                func.execute(arg_values?)
            }))
        } //_ => Err(ExecutionError::Custom(format!("No compiler for {:?}", expr)))
    }
}

pub struct FilterRelation {
    schema: Rc<Schema>,
    input: Box<SimpleRelation>,
    expr: CompiledExpr,
}

pub struct ProjectRelation {
    schema: Rc<Schema>,
    input: Box<SimpleRelation>,
    expr: Vec<CompiledExpr>,
}

//pub struct SortRelation {
//    schema: Schema,
//    input: Box<SimpleRelation>,
//    sort_expr: Vec<CompiledExpr>,
//    sort_asc: Vec<bool>
//}

pub struct LimitRelation {
    schema: Rc<Schema>,
    input: Box<SimpleRelation>,
    limit: usize,
}

/// trait for all relations (a relation is essentially just an iterator over rows with
/// a known schema)
pub trait SimpleRelation {
    /// scan all records in this relation
    fn scan<'a>(&'a mut self)
        -> Box<Iterator<Item = Result<Rc<RecordBatch>, ExecutionError>> + 'a>;

    /// get the schema for this relation
    fn schema<'a>(&'a self) -> &'a Schema;
}

struct DataSourceRelation {
    schema: Schema,
    ds: Rc<RefCell<DataSource>>,
}

impl SimpleRelation for DataSourceRelation {
    fn scan<'a>(
        &'a mut self,
    ) -> Box<Iterator<Item = Result<Rc<RecordBatch>, ExecutionError>> + 'a> {
        Box::new(DataSourceIterator::new(self.ds.clone()))
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}

impl SimpleRelation for FilterRelation {
    fn scan<'a>(
        &'a mut self,
    ) -> Box<Iterator<Item = Result<Rc<RecordBatch>, ExecutionError>> + 'a> {
        let filter_expr = &self.expr;
        let schema = self.schema.clone();

        Box::new(self.input.scan().map(move |b| {
            match b {
                Ok(ref batch) => {
                    // evaluate the filter expression for every row in the batch
                    let x = (*filter_expr)(batch.as_ref())?;
                    match x.as_ref() {
                        &Value::Column(ref filter_eval) => {
                            let filtered_columns: Vec<Rc<Value>> = (0..batch.num_columns())
                                .map(move |column_index| {
                                    let column = batch.column(column_index);
                                    Rc::new(Value::Column(Rc::new(filter(column, &filter_eval))))
                                })
                                .collect();

                            let row_count_opt: Option<usize> = filtered_columns
                                .iter()
                                .map(|c| match c.as_ref() {
                                    &Value::Scalar(_) => 1,
                                    &Value::Column(ref v) => v.len(),
                                })
                                .max();

                            //TODO: should ge able to something like `row_count_opt.or_else(0)` ?
                            let row_count = match row_count_opt {
                                None => 0,
                                Some(n) => n,
                            };

                            let filtered_batch: Rc<RecordBatch> = Rc::new(DefaultRecordBatch {
                                row_count,
                                data: filtered_columns,
                                schema: schema.clone(),
                            });

                            Ok(filtered_batch)
                        }
                        _ => panic!(),
                    }
                }
                _ => panic!(),
            }
        }))
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}

impl SimpleRelation for ProjectRelation {
    fn scan<'a>(
        &'a mut self,
    ) -> Box<Iterator<Item = Result<Rc<RecordBatch>, ExecutionError>> + 'a> {
        let project_expr = &self.expr;

        let projection_iter = self.input.scan().map(move |r| match r {
            Ok(ref batch) => {
                let projected_columns: Result<Vec<Rc<Value>>, ExecutionError> =
                    project_expr.iter().map(|e| (*e)(batch.as_ref())).collect();

                let projected_batch: Rc<RecordBatch> = Rc::new(DefaultRecordBatch {
                    schema: Rc::new(Schema::empty()), //TODO
                    data: projected_columns?,
                    row_count: batch.num_rows(),
                });

                Ok(projected_batch)
            }
            Err(_) => r,
        });

        Box::new(projection_iter)
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        unimplemented!()
    }
}

impl SimpleRelation for LimitRelation {
    fn scan<'a>(
        &'a mut self,
    ) -> Box<Iterator<Item = Result<Rc<RecordBatch>, ExecutionError>> + 'a> {
        unimplemented!()
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        unimplemented!()
    }
}

/// Execution plans are sent to worker nodes for execution
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Run a query and return the results to the client
    Interactive {
        plan: Box<LogicalPlan>,
    },
    /// Execute a logical plan and write the output to a file
    Write {
        plan: Rc<LogicalPlan>,
        filename: String,
    },
    Show {
        plan: Rc<LogicalPlan>,
        count: usize,
    },
}

#[derive(Debug, Clone)]
pub enum ExecutionResult {
    Unit,
    Count(usize),
}

#[derive(Clone)]
pub struct ExecutionContext {
    schemas: Rc<RefCell<HashMap<String, Rc<Schema>>>>,
    function_meta: Rc<RefCell<HashMap<String, FunctionMeta>>>,
    functions: Rc<RefCell<HashMap<String, Rc<ScalarFunction>>>>,
    config: Rc<DFConfig>,
    tables: Rc<RefCell<HashMap<String, Rc<DataFrame>>>>,
}

impl ExecutionContext {
    pub fn local() -> Self {
        ExecutionContext {
            schemas: Rc::new(RefCell::new(HashMap::new())),
            function_meta: Rc::new(RefCell::new(HashMap::new())),
            functions: Rc::new(RefCell::new(HashMap::new())),
            tables: Rc::new(RefCell::new(HashMap::new())),
            config: Rc::new(DFConfig::Local),
        }
    }

    pub fn remote(_etcd: String) -> Self {
        unimplemented!("this feature is disabled at the moment")
        //        ExecutionContext {
        //            schemas: Rc::new(RefCell::new(HashMap::new())),
        //            function_meta: Rc::new(RefCell::new(HashMap::new())),
        //            tables: Rc::new(RefCell::new(HashMap::new())),
        //            config: Rc::new(DFConfig::Remote { etcd: etcd }),
        //        }
    }

    pub fn define_schema(&mut self, name: &str, schema: &Schema) {
        self.schemas
            .borrow_mut()
            .insert(name.to_string(), Rc::new(schema.clone()));
    }

    pub fn register_function(&mut self, func: Rc<ScalarFunction>) {
        let fm = FunctionMeta {
            name: func.name(),
            args: func.args(),
            return_type: func.return_type(),
        };

        self.function_meta
            .borrow_mut()
            .insert(func.name().to_lowercase(), fm);

        self.functions
            .borrow_mut()
            .insert(func.name().to_lowercase(), func.clone());
    }

    pub fn create_logical_plan(&self, sql: &str) -> Result<Rc<LogicalPlan>, ExecutionError> {
        // parse SQL into AST
        let ast = Parser::parse_sql(String::from(sql))?;

        // create a query planner
        let query_planner = SqlToRel::new(self.schemas.clone()); //TODO: pass reference to schemas

        // plan the query (create a logical relational plan)
        Ok(query_planner.sql_to_rel(&ast)?)
    }

    pub fn register(&mut self, table_name: &str, df: Rc<DataFrame>) {
        //println!("Registering table {}", table_name);
        self.tables
            .borrow_mut()
            .insert(table_name.to_string(), df.clone());

        // temp hack
        self.schemas
            .borrow_mut()
            .insert(table_name.to_string(), df.schema().clone());
    }

    pub fn sql(&mut self, sql: &str) -> Result<Rc<DataFrame>, ExecutionError> {
        //println!("sql() {}", sql);

        // parse SQL into AST
        let ast = Parser::parse_sql(String::from(sql))?;

        match ast {
            SQLCreateTable { name, columns } => {
                let fields: Vec<Field> = columns
                    .iter()
                    .map(|c| Field::new(&c.name, convert_data_type(&c.data_type), c.allow_null))
                    .collect();
                let schema = Schema::new(fields);
                self.define_schema(&name, &schema);

                //TODO: not sure what to return here
                Ok(Rc::new(DF {
                    ctx: self.clone(),
                    plan: Rc::new(LogicalPlan::EmptyRelation {
                        schema: Rc::new(Schema::empty()),
                    }),
                }))
            }
            _ => {
                // create a query planner
                let query_planner = SqlToRel::new(self.schemas.clone()); //TODO: pass reference to schemas

                // plan the query (create a logical relational plan)
                let plan = query_planner.sql_to_rel(&ast)?;

                // return the DataFrame
                Ok(Rc::new(DF {
                    ctx: self.clone(),
                    plan: plan,
                }))
            }
        }
    }

    /// Open a CSV file
    ///TODO: this is building a relational plan not an execution plan so shouldn't really be here
    pub fn load_csv(
        &self,
        filename: &str,
        schema: &Schema,
    ) -> Result<Rc<DataFrame>, ExecutionError> {
        let plan = LogicalPlan::CsvFile {
            filename: filename.to_string(),
            schema: Rc::new(schema.clone()),
        };
        Ok(Rc::new(DF {
            ctx: self.clone(),
            plan: Rc::new(plan),
        }))
    }

    pub fn load_parquet(&self, filename: &str) -> Result<Rc<DataFrame>, ExecutionError> {
        //TODO: can only get schema by assuming file is local and opening it - need catalog!!
        let file = File::open(filename)?;
        let p = ParquetFile::open(file)?;

        let plan = LogicalPlan::ParquetFile {
            filename: filename.to_string(),
            schema: p.schema().clone(),
        };
        Ok(Rc::new(DF {
            ctx: self.clone(),
            plan: Rc::new(plan),
        }))
    }

    pub fn register_table(&mut self, name: String, schema: Schema) {
        self.schemas
            .borrow_mut()
            .insert(name, Rc::new(schema.clone()));
    }

    pub fn create_execution_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Box<SimpleRelation>, ExecutionError> {
        //println!("Logical plan: {:?}", plan);

        match *plan {
            LogicalPlan::EmptyRelation { .. } => Err(ExecutionError::Custom(String::from(
                "empty relation is not implemented yet",
            ))),

            LogicalPlan::Sort { .. } => unimplemented!(),

            LogicalPlan::TableScan { ref table_name, .. } => {
                //println!("TableScan: {}", table_name);
                match self.tables.borrow().get(table_name) {
                    Some(df) => df.create_execution_plan(),
                    _ => Err(ExecutionError::Custom(format!(
                        "No table registered as '{}'",
                        table_name
                    ))),
                }
            }

            LogicalPlan::CsvFile {
                ref filename,
                ref schema,
            } => {
                let file = File::open(filename)?;
                let ds = Rc::new(RefCell::new(CsvFile::open(file, schema.clone())?))
                    as Rc<RefCell<DataSource>>;
                Ok(Box::new(DataSourceRelation {
                    schema: schema.as_ref().clone(),
                    ds,
                }))
            }

            LogicalPlan::ParquetFile {
                ref filename,
                ref schema,
            } => {
                let file = File::open(filename)?;
                let ds = Rc::new(RefCell::new(ParquetFile::open(file)?)) as Rc<RefCell<DataSource>>;
                Ok(Box::new(DataSourceRelation {
                    schema: schema.as_ref().clone(),
                    ds,
                }))
            }

            LogicalPlan::Selection {
                ref expr,
                ref input,
                ref schema,
            } => {
                let input_rel = self.create_execution_plan(input)?;

                let rel = FilterRelation {
                    input: input_rel,
                    expr: compile_expr(&self, expr)?,
                    schema: schema.clone(),
                };
                Ok(Box::new(rel))
            }

            LogicalPlan::Projection {
                ref expr,
                ref input,
                ..
            } => {
                let input_rel = self.create_execution_plan(&input)?;
                let input_schema = input_rel.schema().clone();

                //TODO: seems to be duplicate of sql_to_rel code
                let project_columns: Vec<Field> = expr.iter()
                    .map(|e| {
                        match e {
                            &Expr::Column(i) => input_schema.columns[i].clone(),
                            &Expr::ScalarFunction { ref name, .. } => Field {
                                name: name.clone(),
                                data_type: DataType::Float64, //TODO: hard-coded .. no function metadata yet
                                nullable: true,
                            },
                            _ => unimplemented!("Unsupported projection expression"),
                        }
                    })
                    .collect();

                let project_schema = Schema {
                    columns: project_columns,
                };

                let compiled_expr: Result<Vec<CompiledExpr>, ExecutionError> =
                    expr.iter().map(|e| compile_expr(&self, e)).collect();

                let rel = ProjectRelation {
                    input: input_rel,
                    expr: compiled_expr?,
                    schema: Rc::new(project_schema),
                };

                Ok(Box::new(rel))
            }

            //            LogicalPlan::Sort { ref expr, ref input, ref schema } => {
            //                let input_rel = self.create_execution_plan(data_dir, input)?;
            //
            //                let compiled_expr : Result<Vec<CompiledExpr>, ExecutionError> = expr.iter()
            //                    .map(|e| compile_expr(&self,e))
            //                    .collect();
            //
            //                let sort_asc : Vec<bool> = expr.iter()
            //                    .map(|e| match e {
            //                        &Expr::Sort { asc, .. } => asc,
            //                        _ => panic!()
            //                    })
            //                    .collect();
            //
            //                let rel = SortRelation {
            //                    input: input_rel,
            //                    sort_expr: compiled_expr?,
            //                    sort_asc: sort_asc,
            //                    schema: schema.clone()
            //                };
            //                Ok(Box::new(rel))
            //            },
            LogicalPlan::Limit {
                limit,
                ref input,
                ref schema,
                ..
            } => {
                let input_rel = self.create_execution_plan(input)?;
                let rel = LimitRelation {
                    input: input_rel,
                    limit: limit,
                    schema: schema.clone(),
                };
                Ok(Box::new(rel))
            }
        }
    }

    /// load a function implementation
    fn load_function_impl(
        &self,
        function_name: &str,
    ) -> Result<Rc<ScalarFunction>, ExecutionError> {
        match self.functions.borrow().get(&function_name.to_lowercase()) {
            Some(f) => Ok(f.clone()),
            _ => Err(ExecutionError::Custom(format!(
                "Unknown function {}",
                function_name
            ))),
        }
    }

    pub fn udf(&self, name: &str, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction {
            name: name.to_string(),
            args: args.clone(),
        }
    }

    pub fn show(&self, df: &DataFrame, count: usize) -> Result<usize, DataFrameError> {
        //println!("show()");
        let physical_plan = PhysicalPlan::Show {
            plan: df.plan().clone(),
            count,
        };

        match self.execute(&physical_plan)? {
            ExecutionResult::Count(count) => Ok(count),
            _ => Err(DataFrameError::NotImplemented), //TODO better error
        }
    }

    pub fn write_csv(&self, df: Rc<DataFrame>, filename: &str) -> Result<usize, DataFrameError> {
        let physical_plan = PhysicalPlan::Write {
            plan: df.plan().clone(),
            filename: filename.to_string(),
        };

        match self.execute(&physical_plan)? {
            ExecutionResult::Count(count) => Ok(count),
            _ => Err(DataFrameError::NotImplemented), //TODO better error
        }
    }

    pub fn execute(&self, physical_plan: &PhysicalPlan) -> Result<ExecutionResult, ExecutionError> {
        //println!("execute()");
        match &self.config.as_ref() {
            &DFConfig::Local => {
                //TODO error handling
                match self.execute_local(physical_plan) {
                    Ok(r) => Ok(r),
                    Err(e) => Err(ExecutionError::Custom(format!("execution failed: {:?}", e))),
                }
            }
            &DFConfig::Remote { ref etcd } => self.execute_remote(physical_plan, etcd.clone()),
        }
    }

    fn execute_local(
        &self,
        physical_plan: &PhysicalPlan,
    ) -> Result<ExecutionResult, ExecutionError> {
        //println!("execute_local()");

        match physical_plan {
            &PhysicalPlan::Interactive { .. } => {
                Err(ExecutionError::Custom(format!("not implemented")))
            }
            &PhysicalPlan::Write {
                ref plan,
                ref filename,
            } => {
                // create output file
                // //println!("Writing csv to {}", filename);
                let file = File::create(filename)?;

                let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

                let mut execution_plan = self.create_execution_plan(plan)?;

                // implement execution here for now but should be a common method for processing a plan
                let it = execution_plan.scan();
                let mut count: usize = 0;
                it.for_each(|t| {
                    match t {
                        Ok(ref batch) => {
                            ////println!("Processing batch of {} rows", batch.row_count());
                            for i in 0..batch.num_rows() {
                                let row = batch.row_slice(i);
                                let csv = row.into_iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<String>>()
                                    .join(",");
                                writer.write(&csv.into_bytes()).unwrap();
                                writer.write(b"\n").unwrap();
                                count += 1;
                            }
                        }
                        Err(e) => panic!(format!("Error processing row: {:?}", e)), //TODO: error handling
                    }
                });

                Ok(ExecutionResult::Count(count))
            }
            &PhysicalPlan::Show {
                ref plan,
                ref count,
            } => {
                let mut execution_plan = self.create_execution_plan(plan)?;

                // implement execution here for now but should be a common method for processing a plan
                let it = execution_plan.scan().take(*count);
                it.for_each(|t| {
                    match t {
                        Ok(ref batch) => {
                            ////println!("Processing batch of {} rows", batch.row_count());
                            for i in 0..*count {
                                if i < batch.num_rows() {
                                    let row = batch.row_slice(i);
                                    let csv = row.into_iter()
                                        .map(|v| v.to_string())
                                        .collect::<Vec<String>>()
                                        .join(",");
                                    println!("{}", csv);
                                }
                            }
                        }
                        Err(e) => panic!(format!("Error processing row: {:?}", e)), //TODO: error handling
                    }
                });

                Ok(ExecutionResult::Count(*count))
            }
        }
    }

    fn execute_remote(
        &self,
        _physical_plan: &PhysicalPlan,
        _etcd: String,
    ) -> Result<ExecutionResult, ExecutionError> {
        Err(ExecutionError::Custom(format!(
            "Remote execution needs re-implementing since moving to Arrow"
        )))
    }

    //        let workers = get_worker_list(&etcd);
    //
    //        match workers {
    //            Ok(ref list) if list.len() > 0 => {
    //                let worker_uri = format!("http://{}", list[0]);
    //                match worker_uri.parse() {
    //                    Ok(uri) => {
    //
    //                        let mut core = Core::new().unwrap();
    //                        let client = Client::new(&core.handle());
    //
    //                        // serialize plan to JSON
    //                        match serde_json::to_string(&physical_plan) {
    //                            Ok(json) => {
    //                                let mut req = Request::new(Method::Post, uri);
    //                                req.headers_mut().set(ContentType::json());
    //                                req.headers_mut().set(ContentLength(json.len() as u64));
    //                                req.set_body(json);
    //
    //                                let post = client.request(req).and_then(|res| {
    //                                    ////println!("POST: {}", res.status());
    //                                    res.body().concat2()
    //                                });
    //
    //                                match core.run(post) {
    //                                    Ok(result) => {
    //                                        //TODO: parse result
    //                                        let result = str::from_utf8(&result).unwrap();
    //                                        //println!("{}", result);
    //                                        Ok(ExecutionResult::Unit)
    //                                    }
    //                                    Err(e) => Err(ExecutionError::Custom(format!("error: {}", e)))
    //                                }
    //                            }
    //                            Err(e) => Err(ExecutionError::Custom(format!("error: {}", e)))
    //                        }
    //
    //
    //                    }
    //                    Err(e) => Err(ExecutionError::Custom(format!("error: {}", e)))
    //                }
    //            }
    //            Ok(_) => Err(ExecutionError::Custom(format!("No workers found in cluster"))),
    //            Err(e) => Err(ExecutionError::Custom(format!("Failed to find a worker node: {}", e)))
    //        }
    //    }
}

pub struct DF {
    ctx: ExecutionContext,
    pub plan: Rc<LogicalPlan>,
}

impl DF {
    pub fn new(ctx: ExecutionContext, plan: Rc<LogicalPlan>) -> Self {
        DF { ctx, plan }
    }

    pub fn with_plan(&self, plan: Rc<LogicalPlan>) -> Self {
        DF::new(self.ctx.clone(), plan)
    }
}

impl DataFrame for DF {
    fn select(&self, expr: Vec<Expr>) -> Result<Rc<DataFrame>, DataFrameError> {
        let plan = LogicalPlan::Projection {
            expr: expr,
            input: self.plan.clone(),
            schema: self.plan.schema().clone(),
        };

        Ok(Rc::new(self.with_plan(Rc::new(plan))))
    }

    fn sort(&self, expr: Vec<Expr>) -> Result<Rc<DataFrame>, DataFrameError> {
        let plan = LogicalPlan::Sort {
            expr: expr,
            input: self.plan.clone(),
            schema: self.plan.schema().clone(),
        };

        Ok(Rc::new(self.with_plan(Rc::new(plan))))
    }

    fn filter(&self, expr: Expr) -> Result<Rc<DataFrame>, DataFrameError> {
        let plan = LogicalPlan::Selection {
            expr: expr,
            input: self.plan.clone(),
            schema: self.plan.schema().clone(),
        };

        Ok(Rc::new(self.with_plan(Rc::new(plan))))
    }

    fn col(&self, column_name: &str) -> Result<Expr, DataFrameError> {
        match self.plan.schema().column(column_name) {
            Some((i, _)) => Ok(Expr::Column(i)),
            _ => Err(DataFrameError::InvalidColumn(column_name.to_string())),
        }
    }

    fn schema(&self) -> &Rc<Schema> {
        self.plan.schema()
    }

    fn plan(&self) -> &Rc<LogicalPlan> {
        &self.plan
    }

    fn show(&self, count: usize) {
        self.ctx.show(self, count).unwrap();
    }

    fn create_execution_plan(&self) -> Result<Box<SimpleRelation>, ExecutionError> {
        self.ctx.create_execution_plan(&self.plan)
    }
}

pub fn filter(column: &Value, bools: &Array) -> Array {
    match column {
        &Value::Scalar(_) => unimplemented!(),
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
                //&ArrayData::UInt8(ref v) => Array::from(v.iter().zip(b.iter()).filter(|&(_, f)| f).map(|(v, _)| v).collect::<Vec<u8>>()),
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
                    let mut x: Vec<String> = Vec::with_capacity(b.len() as usize);
                    for i in 0..b.len() as usize {
                        if *b.get(i) {
                            x.push(String::from_utf8(v.slice(i as usize).to_vec()).unwrap());
                        }
                    }
                    Array::from(x)
                }
                _ => unimplemented!(),
            },
            _ => panic!(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::super::functions::geospatial::*;
    use super::super::functions::math::*;
    use super::*;

    #[test]
    fn test_sqrt() {
        let mut ctx = create_context();

        ctx.register_function(Rc::new(SqrtFunction {}));

        let df = ctx.sql(&"SELECT id, sqrt(id) FROM people").unwrap();

        //println!("Logical plan: {:?}", df.plan());

        ctx.write_csv(df, "_sqrt_out.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    #[test]
    fn test_sql_udf_udt() {
        let mut ctx = create_context();

        ctx.register_function(Rc::new(STPointFunc {}));

        let df = ctx.sql(&"SELECT ST_Point(lat, lng) FROM uk_cities")
            .unwrap();

        ctx.write_csv(df, "_uk_cities_sql.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    #[test]
    fn test_df_udf_udt() {
        let mut ctx = create_context();

        ctx.register_function(Rc::new(STPointFunc {}));

        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let df = ctx.load_csv("test/data/uk_cities.csv", &schema).unwrap();

        // create an expression for invoking a scalar function
        //        let func_expr = Expr::ScalarFunction {
        //            name: "ST_Point".to_string(),
        //            args: vec![df.col("lat").unwrap(), df.col("lng").unwrap()]
        //        };

        // invoke custom code as a scalar UDF
        let func_expr = ctx.udf(
            "ST_Point",
            vec![df.col("lat").unwrap(), df.col("lng").unwrap()],
        );

        let df2 = df.select(vec![func_expr]).unwrap();

        ctx.write_csv(df2, "_uk_cities_df.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    #[test]
    fn test_filter() {
        let mut ctx = create_context();

        ctx.register_function(Rc::new(STPointFunc {}));

        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let df = ctx.load_csv("test/data/uk_cities.csv", &schema).unwrap();

        // filter by lat
        let df2 = df.filter(Expr::BinaryExpr {
            left: Rc::new(Expr::Column(1)), // lat
            op: Operator::Gt,
            right: Rc::new(Expr::Literal(ScalarValue::Float64(52.0))),
        }).unwrap();

        ctx.write_csv(df2, "_uk_cities_filtered_gt_52.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    /*
    #[test]
    fn test_sort() {

        let mut ctx = create_context();

        ctx.define_function(&STPointFunc {});

        let schema = Schema::new(vec![
            Field::new("city", DataType::String, false),
            Field::new("lat", DataType::Double, false),
            Field::new("lng", DataType::Double, false)]);

        let df = ctx.load("test/data/uk_cities.csv", &schema).unwrap();

        // sort by lat, lng ascending
        let df2 = df.sort(vec![
            Expr::Sort { expr: Box::new(Expr::Column(1)), asc: true },
            Expr::Sort { expr: Box::new(Expr::Column(2)), asc: true }
        ]).unwrap();

        ctx.write(df2,"_uk_cities_sorted_by_lat_lng.csv").unwrap();

        //TODO: check that generated file has expected contents
    }
    */

    #[test]
    fn test_chaining_functions() {
        let mut ctx = create_context();
        ctx.register_function(Rc::new(STPointFunc {}));
        ctx.register_function(Rc::new(STAsText {}));

        ctx.register_function(Rc::new(STPointFunc {}));

        let df = ctx.sql(&"SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities")
            .unwrap();

        ctx.write_csv(df, "_uk_cities_wkt.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    fn create_context() -> ExecutionContext {
        // create execution context
        let mut ctx = ExecutionContext::local();

        let people = ctx.load_csv(
            "./test/data/people.csv",
            &Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]),
        ).unwrap();

        ctx.register("people", people);

        let uk_cities = ctx.load_csv(
            "./test/data/uk_cities.csv",
            &Schema::new(vec![
                Field::new("city", DataType::Utf8, false),
                Field::new("lat", DataType::Float64, false),
                Field::new("lng", DataType::Float64, false),
            ]),
        ).unwrap();

        ctx.register("uk_cities", uk_cities);

        ctx
    }

    #[test]
    fn sql_query_example() {
        // create execution context
        let mut ctx = ExecutionContext::local();
        ctx.register_function(Rc::new(STPointFunc {}));
        ctx.register_function(Rc::new(STAsText {}));

        // define an external table (csv file)
        //        ctx.sql(
        //            "CREATE EXTERNAL TABLE uk_cities (\
        //             city VARCHAR(100), \
        //             lat DOUBLE, \
        //             lng DOUBLE)",
        //        ).unwrap();

        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let df = ctx.load_csv("./test/data/uk_cities.csv", &schema).unwrap();
        ctx.register("uk_cities", df);

        // define the SQL statement
        let sql = "SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities WHERE lat < 53.0";

        // create a data frame
        let df1 = ctx.sql(&sql).unwrap();

        // write the results to a file
        ctx.write_csv(df1, "_southern_cities.csv").unwrap();
    }
}
