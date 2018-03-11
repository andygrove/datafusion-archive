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

use std::clone::Clone;
use std::cmp::Ordering::*;
use std::collections::HashMap;
use std::io::{BufWriter, Error};
use std::io::prelude::*;
use std::iter::Iterator;
use std::fs::File;
use std::str;
use std::string::String;
use std::convert::*;

extern crate futures;
extern crate hyper;
extern crate serde;
extern crate serde_json;
extern crate tokio_core;

use self::futures::{Future, Stream};
use self::hyper::Client;
use self::tokio_core::reactor::Core;
use self::hyper::{Method, Request};
use self::hyper::header::{ContentLength, ContentType};

use super::api::*;
use super::datasource::csv::CsvRelation;
use super::rel::*;
use super::sql::ASTNode::*;
use super::sqltorel::*;
use super::parser::*;
use super::cluster::*;
use super::dataframe::*;
use super::functions::math::*;
use super::functions::geospatial::*;

#[derive(Debug,Clone)]
pub enum DFConfig {
    Local { data_dir: String },
    Remote { etcd: String },
}

#[derive(Debug)]
pub enum ExecutionError {
    IoError(Error),
    ParserError(ParserError),
    Custom(String)
}

impl From<Error> for ExecutionError {
    fn from(e: Error) -> Self {
        ExecutionError::IoError(e)
    }
}


impl From<String> for ExecutionError {
    fn from(e: String) -> Self {
        ExecutionError::Custom(e)
    }
}

impl From<ParserError> for ExecutionError {
    fn from(e: ParserError) -> Self {
        ExecutionError::ParserError(e)
    }
}

/// A batch of data organized as columns. The intent is to implement this using Apache Arrow
/// soon but for now there will be a naive version while I integrate this.
pub trait Batch {
    fn col_count(&self) -> usize;
    fn row_count(&self) -> usize;
    fn column(&self, index: usize) -> &ColumnData;

    /// access a row
    fn row_slice(&self, index: usize) -> Vec<Value>;
}

pub struct ColumnBatch {
    pub columns: Vec<ColumnData>
}

//impl ColumnBatch {
//
//    pub fn from_rows(rows: Vec<Vec<Value>>) -> Self {
//        let column_count = rows[0].len();
//        let columns : Vec<ColumnData> = (0 .. column_count)
//            .map(|i| rows.iter().map(|r| (&r[i]).clone()).collect() )
//            .collect();
//        ColumnBatch { columns }
//    }
//}

impl Batch for ColumnBatch {
    fn col_count(&self) -> usize {
        self.columns.len()
    }

    fn row_count(&self) -> usize {
        self.columns[0].len()
    }

    fn column(&self, index: usize) -> &ColumnData {
        &self.columns[index]
    }

    fn row_slice(&self, index: usize) -> Vec<Value> {
        self.columns.iter().map(|c| match c {
            &ColumnData::Boolean(ref v) => Value::Boolean(v[index]),
            &ColumnData::Float(ref v) => Value::Float(v[index]),
            &ColumnData::Double(ref v) => Value::Double(v[index]),
            &ColumnData::Long(ref v) => Value::Long(v[index]),
            &ColumnData::UnsignedLong(ref v) => Value::UnsignedLong(v[index]),
            &ColumnData::String(ref v) => Value::String(v[index].clone()),
            //&ColumnData::ComplexValue(ref v) => Value::ComplexValue(v[index].clone()),
            _ => unimplemented!()
        }).collect()
    }
}

#[derive(Clone)]
pub enum ColumnData {
    BroadcastVariable(Value),
    Boolean(Vec<bool>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    UnsignedInt(Vec<u32>),
    Long(Vec<i64>),
    UnsignedLong(Vec<u64>),
    String(Vec<String>),
    ComplexValue(Vec<ColumnData>)
}

impl From<Vec<Value>> for ColumnData {
    fn from(vec: Vec<Value>) -> Self {
        match &vec[0] {
            &Value::Boolean(_) => ColumnData::Boolean(vec.iter().map(|v| match v { &Value::Boolean(v) => v, _ => panic!() }).collect()),
            &Value::Float(_) => ColumnData::Float(vec.iter().map(|v| match v { &Value::Float(v) => v, _ => panic!() }).collect()),
            &Value::Double(_) => ColumnData::Double(vec.iter().map(|v| match v { &Value::Double(v) => v, _ => panic!() }).collect()),
            &Value::Long(_) => ColumnData::Long(vec.iter().map(|v| match v { &Value::Long(v) => v, _ => panic!() }).collect()),
            &Value::UnsignedLong(_) => ColumnData::UnsignedLong(vec.iter().map(|v| match v { &Value::UnsignedLong(v) => v, _ => panic!() }).collect()),
            &Value::String(_) => ColumnData::String(vec.iter().map(|v| match v { &Value::String(ref v) => v.clone(), _ => panic!() }).collect()),
            &Value::ComplexValue(_) => ColumnData::ComplexValue(vec.iter().map(|v| match v { &Value::ComplexValue(ref v) => ColumnData::from(v.clone()), _ => panic!() }).collect()),
        }
    }
}

impl ColumnData {

    pub fn len(&self) -> usize {
        match self {
            &ColumnData::BroadcastVariable(_) => 1,
            &ColumnData::Boolean(ref v) => v.len(),
            &ColumnData::Float(ref v) => v.len(),
            &ColumnData::Double(ref v) => v.len(),
            &ColumnData::UnsignedInt(ref v) => v.len(),
            &ColumnData::Long(ref v) => v.len(),
            &ColumnData::UnsignedLong(ref v) => v.len(),
            &ColumnData::String(ref v) => v.len(),
            &ColumnData::ComplexValue(ref v) => v.len(),
        }
    }

    pub fn filter(&self, bools: ColumnData) -> ColumnData {
        match bools {
            ColumnData::Boolean(b) => match self {
                &ColumnData::Double(ref v) => ColumnData::Double(
                    v.iter().zip(b.iter())
                        .filter(|&(_,f)| *f)
                        .map(|(v,_)| *v)
                        .collect()
                ),
                _ => unimplemented!()
            },
            _ => panic!()
        }
    }

}

/// Compiled Expression (basically just a closure to evaluate the expression at runtime)
pub type CompiledExpr = Box<Fn(&Batch) -> ColumnData>;

/// Compiles a relational expression into a closure
pub fn compile_expr(ctx: &ExecutionContext, expr: &Expr) -> Result<CompiledExpr, ExecutionError> {
    match expr {
        &Expr::Literal(ref lit) => {
            let literal_value = lit.clone();
            Ok(Box::new(move |_| {
                // literal values are a bit special - we don't repeat them in a vector
                // because it would be redundant, so we have a single value in a vector instead
                ColumnData::BroadcastVariable(literal_value.clone())
            }))
        }
        &Expr::Column(index) => Ok(Box::new(move |batch: &Batch| {
            let c: &ColumnData = batch.column(index);
            (*c).clone()
        })),
        &Expr::BinaryExpr { ref left, ref op, ref right } => {
            let left_expr = compile_expr(ctx,left)?;
            let right_expr = compile_expr(ctx,right)?;
            match op {
                &Operator::Eq => Ok(Box::new(move |batch: &Batch| {
                    let left_values : ColumnData = left_expr(batch);
                    let right_values : ColumnData = right_expr(batch);
                    match (left_values, right_values) {
                        (ColumnData::Double(l), ColumnData::Double(r)) => {
                            ColumnData::Boolean(l.iter().zip(r.iter()).map(|(a,b)| a==b)
                            .collect())
                        },
                        _ => unimplemented!()

                    }
                })),
                &Operator::Lt => Ok(Box::new(move |batch: &Batch| {
                    let left_values : ColumnData = left_expr(batch);
                    let right_values : ColumnData = right_expr(batch);
                    match (left_values, right_values) {
                        (ColumnData::Double(l), ColumnData::Double(r)) => {
                            ColumnData::Boolean(l.iter().zip(r.iter()).map(|(a,b)| a<b)
                                .collect())
                        },
                        _ => unimplemented!()

                    }
                })),
                &Operator::Gt => Ok(Box::new(move |batch: &Batch| {
                    let left_values : ColumnData = left_expr(batch);
                    let right_values : ColumnData = right_expr(batch);
                    match (left_values, right_values) {
                        (ColumnData::Double(l), ColumnData::Double(r)) => {
                            ColumnData::Boolean(l.iter().zip(r.iter()).map(|(a,b)| a>b)
                                .collect())
                        },
                        _ => unimplemented!()

                    }
                })),
//                &Operator::NotEq => Ok(Box::new(move |batch: &Batch| {
//                    let left_values = left_expr(batch);
//                    let right_values = right_expr(batch);
//                    let v = left_values.iter().zip(right_values.iter());
//                    v.map(|(a,b)| Value::Boolean(a!=b)).collect()
//                })),
//                &Operator::Gt => Ok(Box::new(move |batch: &Batch| {
//                    let left_values = left_expr(batch);
//                    let right_values = right_expr(batch);
//                    let v = left_values.iter().zip(right_values.iter());
//                    v.map(|(a,b)| Value::Boolean(a>b)).collect()
//                })),
//                &Operator::GtEq => Ok(Box::new(move |batch: &Batch| {
//                    let left_values = left_expr(batch);
//                    let right_values = right_expr(batch);
//                    let v = left_values.iter().zip(right_values.iter());
//                    v.map(|(a,b)| Value::Boolean(a>=b)).collect()
//                })),
//                &Operator::Lt => Ok(Box::new(move |batch: &Batch| {
//                    let left_values = left_expr(batch);
//                    let right_values = right_expr(batch);
//                    let v = left_values.iter().zip(right_values.iter());
//                    v.map(|(a,b)| Value::Boolean(a<b)).collect()
//                })),
//                &Operator::LtEq => Ok(Box::new(move |batch: &Batch| {
//                    let left_values = left_expr(batch);
//                    let right_values = right_expr(batch);
//                    let v = left_values.iter().zip(right_values.iter());
//                    v.map(|(a,b)| Value::Boolean(a<=b)).collect()
//                })),
                _ => return Err(ExecutionError::Custom(format!("Unsupported binary operator '{:?}'", op)))
            }
        }
        &Expr::Sort { ref expr, .. } => {
            //NOTE sort order is ignored here and is handled during sort execution
            compile_expr(ctx, expr)
        },
        &Expr::ScalarFunction { ref name, ref args } => {

            // evaluate the arguments to the function
            let compiled_args : Result<Vec<CompiledExpr>, ExecutionError> = args.iter()
                .map(|e| compile_expr(ctx,e))
                .collect();

            let compiled_args_ok = compiled_args?;

            let func = ctx.load_function_impl(name.as_ref())?;

            Ok(Box::new(move |batch| {

                let arg_values : Vec<ColumnData> = compiled_args_ok.iter()
                    .map(|expr| expr(batch))
                    .collect();

                // convert into rows to call scalar function until the function trait is
                // update to accept columns

                let mut result : Vec<Value> = Vec::with_capacity(batch.row_count());
//
                for i in 0 .. batch.row_count() {

                    // get args for one row
                    let args: Vec<Value> = arg_values.iter().map(|c| match c {
                        &ColumnData::Double(ref v) => Value::Double(v[i]),
                        &ColumnData::UnsignedLong(ref v) => Value::UnsignedLong(v[i]),
                        &ColumnData::String(ref v) => Value::String(v[i].clone()),
                        _ => unimplemented!()
                    })
                    .collect();

                    result.push(func.execute(args).unwrap());
                }

               ColumnData::from(result)

            }))
        }
        //_ => Err(ExecutionError::Custom(format!("No compiler for {:?}", expr)))
    }
}


pub struct FilterRelation {
    schema: Schema,
    input: Box<SimpleRelation>,
    expr: CompiledExpr
}
pub struct ProjectRelation {
    schema: Schema,
    input: Box<SimpleRelation>,
    expr: Vec<CompiledExpr>
}

pub struct SortRelation {
    schema: Schema,
    input: Box<SimpleRelation>,
    sort_expr: Vec<CompiledExpr>,
    sort_asc: Vec<bool>
}

pub struct LimitRelation {
    schema: Schema,
    input: Box<SimpleRelation>,
    limit: usize,
}

/// trait for all relations (a relation is essentially just an iterator over rows with
/// a known schema)
pub trait SimpleRelation {
    /// scan all records in this relation
    fn scan<'a>(&'a mut self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Box<Batch>,ExecutionError>> + 'a>;
    /// get the schema for this relation
    fn schema<'a>(&'a self) -> &'a Schema;
}

impl SimpleRelation for FilterRelation {

    fn scan<'a>(&'a mut self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Box<Batch>, ExecutionError>> + 'a> {

        Box::new(self.input.scan(ctx).map(move |b| {
            match b {
                Ok(ref batch) => {

//                    // evaluate the filter expression for every row in the batch
//                    let filter_eval: ColumnData = (*self.expr)(batch.as_ref());
//
//                    let filtered_columns : Vec<ColumnData> = (0 .. batch.col_count())
//                        .map(move|column_index| { batch.column(column_index).filter(filter_eval) })
//                        .collect();
//
//                    let filtered_batch : Box<Batch> = Box::new(ColumnBatch { columns: filtered_columns });
//
//                    Ok(filtered_batch)

                    unimplemented!()
                }
                _ => panic!()
            }
        }))
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}

impl SimpleRelation for SortRelation {

    // this needs rewriting completely

    fn scan<'a>(&'a mut self, _ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Box<Batch>, ExecutionError>> + 'a> {

//        // collect entire relation into memory (obviously not scalable!)
//        let batches = self.input.scan(ctx);
//
//        // convert into rows for now but this should be converted to columnar
//        let mut rows : Vec<Vec<Value>> = vec![];
//        batches.for_each(|r| match r {
//            Ok(ref batch) => {
//                for i in 0 .. batch.row_count() {
//                    let row : Vec<&Value> = batch.row_slice(i);
//                    let values : Vec<Value> = row.into_iter()
//                        .map(|v| v.clone())
//                        .collect();
//
//                    rows.push(values);
//                }
//            },
//            _ => {}
//                //return Err(ExecutionError::Custom(format!("TBD")))
//        });
//
//        // now sort them
//        rows.sort_by(|a,b| {
//
//            for i in 0 .. self.sort_expr.len() {
//
//                let evaluate = &self.sort_expr[i];
//                let asc = self.sort_asc[i];
//
//                // ugh, this is ugly - convert rows into column batches to evaluate sort expressions
//
//                let a_value = evaluate(&ColumnBatch::from_rows(vec![a.clone()]));
//                let b_value = evaluate(&ColumnBatch::from_rows(vec![b.clone()]));
//
//                if a_value < b_value {
//                    return if asc { Less } else { Greater };
//                } else if a_value > b_value {
//                    return if asc { Greater } else { Less };
//                }
//            }
//
//            Equal
//        });
//
//        // now turn back into columnar!
//        let result_batch : Box<Batch> = Box::new(ColumnBatch::from_rows(rows));
//        let result_it = vec![Ok(result_batch)].into_iter();
//
//
//        Box::new(result_it)

        unimplemented!()
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}

impl SimpleRelation for ProjectRelation {

    fn scan<'a>(&'a mut self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Box<Batch>, ExecutionError>> + 'a> {

        let project_expr = &self.expr;

        let projection_iter = self.input.scan(ctx).map(move|r| match r {
            Ok(ref batch) => {

                //TODO projection causes columns to be copied ... should be able to avoid that

                let projected_columns : Vec<ColumnData> = project_expr.iter()
                    .map(|e| (*e)(batch.as_ref()))
                    .collect();

                let projected_batch : Box<Batch> = Box::new(ColumnBatch { columns: projected_columns.clone() });

                Ok(projected_batch)
            },
            Err(_) => r
        });

        Box::new(projection_iter)
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}

impl SimpleRelation for LimitRelation {
    fn scan<'a>(&'a mut self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Box<Batch>, ExecutionError>> + 'a> {
        Box::new(self.input.scan(ctx).take(self.limit))
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}

/// Execution plans are sent to worker nodes for execution
#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum PhysicalPlan {
    /// Run a query and return the results to the client
    Interactive { plan: Box<LogicalPlan> },
    /// Execute a logical plan and write the output to a file
    Write { plan: Box<LogicalPlan>, filename: String },
}

#[derive(Debug,Clone)]
pub enum ExecutionResult {
    Unit,
    Count(usize),
}

#[derive(Debug,Clone)]
pub struct ExecutionContext {
    schemas: HashMap<String, Schema>,
    functions: HashMap<String, FunctionMeta>,
    config: DFConfig

}

impl ExecutionContext {

    pub fn local(data_dir: String) -> Self {
        ExecutionContext {
            schemas: HashMap::new(),
            functions: HashMap::new(),
            config: DFConfig::Local { data_dir }
        }
    }

    pub fn remote(etcd: String) -> Self {

        ExecutionContext {
            schemas: HashMap::new(),
            functions: HashMap::new(),
            config: DFConfig::Remote { etcd: etcd }
        }
    }

    pub fn define_schema(&mut self, name: &str, schema: &Schema) {
        self.schemas.insert(name.to_string(), schema.clone());
    }

    pub fn define_function(&mut self, func: &ScalarFunction) {

        let fm = FunctionMeta {
            name: func.name(),
            args: func.args(),
            return_type: func.return_type()
        };

        self.functions.insert(fm.name.to_lowercase(), fm);
    }

    pub fn create_logical_plan(&self, sql: &str) -> Result<Box<LogicalPlan>, ExecutionError> {

        // parse SQL into AST
        let ast = Parser::parse_sql(String::from(sql))?;

        // create a query planner
        let query_planner = SqlToRel::new(self.schemas.clone()); //TODO: pass reference to schemas

        // plan the query (create a logical relational plan)
        Ok(query_planner.sql_to_rel(&ast)?)
    }

    pub fn sql(&mut self, sql: &str) -> Result<Box<DataFrame>, ExecutionError> {

        // parse SQL into AST
        let ast = Parser::parse_sql(String::from(sql))?;

        match ast {
            SQLCreateTable { name, columns } => {
                let fields : Vec<Field> = columns.iter()
                    .map(|c| Field::new(&c.name, convert_data_type(&c.data_type), c.allow_null))
                    .collect();
                let schema = Schema::new(fields);
                self.define_schema(&name, &schema);

                //TODO: not sure what to return here
                Ok(Box::new(DF { plan: Box::new(LogicalPlan::EmptyRelation) })) 


            },
            _ => {
                // create a query planner
                let query_planner = SqlToRel::new(self.schemas.clone()); //TODO: pass reference to schemas

                // plan the query (create a logical relational plan)
                let plan = query_planner.sql_to_rel(&ast)?;

                // return the DataFrame
                Ok(Box::new(DF {  plan: plan }))
            }
        }


    }

    /// Open a CSV file
    ///TODO: this is building a relational plan not an execution plan so shouldn't really be here
    pub fn load(&self, filename: &str, schema: &Schema) -> Result<Box<DataFrame>, ExecutionError> {
        let plan = LogicalPlan::CsvFile { filename: filename.to_string(), schema: schema.clone() };
        Ok(Box::new(DF { plan: Box::new(plan) }))
    }

    pub fn register_table(&mut self, name: String, schema: Schema) {
        self.schemas.insert(name, schema);
    }

    pub fn create_execution_plan(&self, data_dir: String, plan: &LogicalPlan) -> Result<Box<SimpleRelation>,ExecutionError> {
        match *plan {

            LogicalPlan::EmptyRelation => {
                Err(ExecutionError::Custom(String::from("empty relation is not implemented yet")))
            },

            LogicalPlan::TableScan { ref table_name, ref schema, .. } => {
                // for now, tables are csv files
                let filename = format!("{}/{}.csv", data_dir, table_name);
                println!("Reading {}", filename);
                let file = File::open(filename)?;
                let rel = CsvRelation::open(file, schema.clone())?;
                Ok(Box::new(rel))
            },

            LogicalPlan::CsvFile { ref filename, ref schema } => {
                let file = File::open(filename)?;
                let rel = CsvRelation::open(file, schema.clone())?;
                Ok(Box::new(rel))
            },

            LogicalPlan::Selection { ref expr, ref input, ref schema } => {
                let input_rel = self.create_execution_plan(data_dir,input)?;

                let rel = FilterRelation {
                    input: input_rel,
                    expr: compile_expr(&self, expr)?,
                    schema: schema.clone()
                };
                Ok(Box::new(rel))
            },

            LogicalPlan::Projection { ref expr, ref input, .. } => {
                let input_rel = self.create_execution_plan(data_dir,&input)?;
                let input_schema = input_rel.schema().clone();

                //TODO: seems to be duplicate of sql_to_rel code
                let project_columns: Vec<Field> = expr.iter().map(|e| {
                    match e {
                        &Expr::Column(i) => input_schema.columns[i].clone(),
                        &Expr::ScalarFunction {ref name, .. } => Field {
                            name: name.clone(),
                            data_type: DataType::Double, //TODO: hard-coded .. no function metadata yet
                            nullable: true
                        },
                        _ => unimplemented!("Unsupported projection expression")
                    }
                }).collect();

                let project_schema = Schema { columns: project_columns };

                let compiled_expr : Result<Vec<CompiledExpr>, ExecutionError> = expr.iter()
                    .map(|e| compile_expr(&self,e))
                    .collect();

                let rel = ProjectRelation {
                    input: input_rel,
                    expr: compiled_expr?,
                    schema: project_schema,

                };

                Ok(Box::new(rel))
            }

            LogicalPlan::Sort { ref expr, ref input, ref schema } => {
                let input_rel = self.create_execution_plan(data_dir, input)?;

                let compiled_expr : Result<Vec<CompiledExpr>, ExecutionError> = expr.iter()
                    .map(|e| compile_expr(&self,e))
                    .collect();

                let sort_asc : Vec<bool> = expr.iter()
                    .map(|e| match e {
                        &Expr::Sort { asc, .. } => asc,
                        _ => panic!()
                    })
                    .collect();

                let rel = SortRelation {
                    input: input_rel,
                    sort_expr: compiled_expr?,
                    sort_asc: sort_asc,
                    schema: schema.clone()
                };
                Ok(Box::new(rel))
            },

            LogicalPlan::Limit { limit, ref input, ref schema, .. } => {
                let input_rel = self.create_execution_plan(data_dir,input)?;
                let rel = LimitRelation {
                    input: input_rel,
                    limit: limit,
                    schema: schema.clone()
                };
                Ok(Box::new(rel))
            }
        }
    }

    /// load a function implementation
    fn load_function_impl(&self, function_name: &str) -> Result<Box<ScalarFunction>,ExecutionError> {

        //TODO: this is a huge hack since the functions have already been registered with the
        // execution context ... I need to implement this so it dynamically loads the functions

        match function_name.to_lowercase().as_ref() {
            "sqrt" => Ok(Box::new(SqrtFunction {})),
            "st_point" => Ok(Box::new(STPointFunc {})),
            "st_astext" => Ok(Box::new(STAsText {})),
            _ => Err(ExecutionError::Custom(format!("Unknown function {}", function_name)))
        }
    }

    pub fn udf(&self, name: &str, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction { name: name.to_string(), args: args.clone() }
    }

    pub fn write(&self, df: Box<DataFrame>, filename: &str) -> Result<usize, DataFrameError> {

        let physical_plan = PhysicalPlan::Write {
            plan: df.plan().clone(),
            filename: filename.to_string()
        };

        match self.execute(&physical_plan)? {
            ExecutionResult::Count(count) => Ok(count),
            _ => Err(DataFrameError::NotImplemented) //TODO better error
        }

    }

//    pub fn collect(&self, df: Box<DataFrame>, filename: &str) -> Result<usize, DataFrameError> {
//
//        let physical_plan = PhysicalPlan::Write {
//            plan: df.plan().clone(),
//            filename: filename.to_string()
//        };
//
//        match self.execute(&physical_plan)? {
//            ExecutionResult::Count(count) => Ok(count),
//            _ => Err(DataFrameError::NotImplemented) //TODO better error
//        }
//
//    }

    pub fn execute(&self, physical_plan: &PhysicalPlan) -> Result<ExecutionResult, ExecutionError> {
        match &self.config {
            &DFConfig::Local { ref data_dir } => self.execute_local(physical_plan, data_dir.clone()),
            &DFConfig::Remote { ref etcd } => self.execute_remote(physical_plan, etcd.clone())
        }
    }

    fn execute_local(&self, physical_plan: &PhysicalPlan, data_dir: String) -> Result<ExecutionResult, ExecutionError> {
        match physical_plan {
            &PhysicalPlan::Interactive { .. } => {
                Err(ExecutionError::Custom(format!("not implemented")))
            }
            &PhysicalPlan::Write { ref plan, ref filename} => {
                // create output file
                // println!("Writing csv to {}", filename);
                let file = File::create(filename)?;

                let mut writer = BufWriter::with_capacity(8*1024*1024,file);

                let mut execution_plan = self.create_execution_plan(data_dir, plan)?;

                // implement execution here for now but should be a common method for processing a plan
                let it = execution_plan.scan(self);
                let mut count : usize = 0;
                it.for_each(|t| {
                    match t {
                        Ok(ref batch) => {
                            for i in 0 .. batch.row_count() {
                                let row = batch.row_slice(i);
                                let csv = row.into_iter().map(|v| v.to_string()).collect::<Vec<String>>().join(",");
                                writer.write(&csv.into_bytes()).unwrap();
                                count += 1;
                            }
                        },
                        Err(e) => panic!(format!("Error processing row: {:?}", e)) //TODO: error handling
                    }
                });

                Ok(ExecutionResult::Count(count))
            }
        }
    }

    fn execute_remote(&self, physical_plan: &PhysicalPlan, etcd: String) -> Result<ExecutionResult, ExecutionError> {
        let workers = get_worker_list(&etcd);

        match workers {
            Ok(ref list) if list.len() > 0 => {
                let worker_uri = format!("http://{}", list[0]);
                match worker_uri.parse() {
                    Ok(uri) => {

                        let mut core = Core::new().unwrap();
                        let client = Client::new(&core.handle());

                        // serialize plan to JSON
                        match serde_json::to_string(&physical_plan) {
                            Ok(json) => {
                                let mut req = Request::new(Method::Post, uri);
                                req.headers_mut().set(ContentType::json());
                                req.headers_mut().set(ContentLength(json.len() as u64));
                                req.set_body(json);

                                let post = client.request(req).and_then(|res| {
                                    //println!("POST: {}", res.status());
                                    res.body().concat2()
                                });

                                match core.run(post) {
                                    Ok(result) => {
                                        //TODO: parse result
                                        let result = str::from_utf8(&result).unwrap();
                                        println!("{}", result);
                                        Ok(ExecutionResult::Unit)
                                    }
                                    Err(e) => Err(ExecutionError::Custom(format!("error: {}", e)))
                                }
                            }
                            Err(e) => Err(ExecutionError::Custom(format!("error: {}", e)))
                        }
                    }
                    Err(e) => Err(ExecutionError::Custom(format!("error: {}", e)))
                }
            }
            Ok(_) => Err(ExecutionError::Custom(format!("No workers found in cluster"))),
            Err(e) => Err(ExecutionError::Custom(format!("Failed to find a worker node: {}", e)))
        }
    }
}

pub struct DF {
    pub plan: Box<LogicalPlan>
}

impl DataFrame for DF {

    fn select(&self, expr: Vec<Expr>) -> Result<Box<DataFrame>, DataFrameError> {

        let plan = LogicalPlan::Projection {
            expr: expr,
            input: self.plan.clone(),
            schema: self.plan.schema().clone()

        };

        Ok(Box::new(DF { plan: Box::new(plan) }))

    }

    fn sort(&self, expr: Vec<Expr>) -> Result<Box<DataFrame>, DataFrameError> {

        let plan = LogicalPlan::Sort {
            expr: expr,
            input: self.plan.clone(),
            schema: self.plan.schema().clone()

        };

        Ok(Box::new(DF { plan: Box::new(plan) }))

    }

    fn filter(&self, expr: Expr) -> Result<Box<DataFrame>, DataFrameError> {

        let plan = LogicalPlan::Selection {
            expr: expr,
            input: self.plan.clone(),
            schema: self.plan.schema().clone()
        };

        Ok(Box::new(DF { plan: Box::new(plan) }))
    }

    fn col(&self, column_name: &str) -> Result<Expr, DataFrameError> {
        match self.plan.schema().column(column_name) {
            Some((i,_)) => Ok(Expr::Column(i)),
            _ => Err(DataFrameError::InvalidColumn(column_name.to_string()))
        }
    }

    fn schema(&self) -> Schema {
        self.plan.schema().clone()
    }


    fn repartition(&self, _n: u32) -> Result<Box<DataFrame>, DataFrameError> {
        unimplemented!()
    }

    fn plan(&self) -> Box<LogicalPlan> {
        self.plan.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqrt() {

        let mut ctx = create_context();

        ctx.define_function(&SqrtFunction {});

        let df = ctx.sql(&"SELECT id, sqrt(id) FROM people").unwrap();

        ctx.write(df,"_sqrt_out.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    #[test]
    fn test_sql_udf_udt() {

        let mut ctx = create_context();

        ctx.define_function(&STPointFunc {});

        let df = ctx.sql(&"SELECT ST_Point(lat, lng) FROM uk_cities").unwrap();

        ctx.write(df,"_uk_cities_sql.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    #[test]
    fn test_df_udf_udt() {

        let mut ctx = create_context();

        ctx.define_function(&STPointFunc {});

        let schema = Schema::new(vec![
            Field::new("city", DataType::String, false),
            Field::new("lat", DataType::Double, false),
            Field::new("lng", DataType::Double, false)]);

        let df = ctx.load("test/data/uk_cities.csv", &schema).unwrap();

        // create an expression for invoking a scalar function
//        let func_expr = Expr::ScalarFunction {
//            name: "ST_Point".to_string(),
//            args: vec![df.col("lat").unwrap(), df.col("lng").unwrap()]
//        };


        // invoke custom code as a scalar UDF
        let func_expr = ctx.udf("ST_Point",vec![
            df.col("lat").unwrap(),
            df.col("lng").unwrap()]
        );

        let df2 = df.select(vec![func_expr]).unwrap();

        ctx.write(df2,"_uk_cities_df.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    #[test]
    fn test_filter() {

        let mut ctx = create_context();

        ctx.define_function(&STPointFunc {});

        let schema = Schema::new(vec![
            Field::new("city", DataType::String, false),
            Field::new("lat", DataType::Double, false),
            Field::new("lng", DataType::Double, false)]);

        let df = ctx.load("test/data/uk_cities.csv", &schema).unwrap();

        // filter by lat
        let df2 = df.filter(Expr::BinaryExpr {
            left: Box::new(Expr::Column(1)), // lat
            op: Operator::Gt,
            right: Box::new(Expr::Literal(Value::Double(52.0)))
        }).unwrap();

        ctx.write(df2,"_uk_cities_filtered_gt_52.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

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

    #[test]
    fn test_chaining_functions() {

        let mut ctx = create_context();

        ctx.define_function(&STPointFunc {});

        let df = ctx.sql(&"SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities").unwrap();

        ctx.write(df,"_uk_cities_wkt.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    fn create_context() -> ExecutionContext {

        // create execution context
        let mut ctx = ExecutionContext::local("./test/data".to_string());

        // define schemas for test data
        ctx.define_schema("people", &Schema::new(vec![
            Field::new("id", DataType::UnsignedLong, false),
            Field::new("name", DataType::String, false)]));

        ctx.define_schema("uk_cities", &Schema::new(vec![
            Field::new("city", DataType::String, false),
            Field::new("lat", DataType::Double, false),
            Field::new("lng", DataType::Double, false)]));

        ctx
    }
}
