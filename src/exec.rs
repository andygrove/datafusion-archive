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

use std::cmp::Ordering::*;
use std::collections::HashMap;
use std::io::Error;
use std::io::BufReader;
use std::io::prelude::*;
use std::iter::Iterator;
use std::fs::File;
use std::string::String;
use std::convert::*;

extern crate csv;

use super::csv::StringRecord;

use super::api::*;
use super::rel::*;
use super::sql::ASTNode::*;
use super::sqltorel::*;
use super::parser::*;
use super::dataframe::*;
use super::functions::math::*;
use super::functions::geospatial::*;

#[derive(Debug)]
pub enum ExecutionError {
    IoError(Error),
    CsvError(csv::Error),
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

/// Compiled Expression (basically just a closure to evaluate the expression at runtime)
pub type CompiledExpr = Box<Fn(&Row)-> Value>;

/// Compiles a relational expression into a closure
pub fn compile_expr(ctx: &ExecutionContext, expr: &Expr) -> Result<CompiledExpr, ExecutionError> {
    match expr {
        &Expr::Literal(ref lit) => {
            let foo = lit.clone();
            Ok(Box::new(move |_| foo.clone()))
        }
        &Expr::TupleValue(index) => Ok(Box::new(move |row| row.values[index].clone())),
        &Expr::BinaryExpr { ref left, ref op, ref right } => {
            let l = compile_expr(ctx,left)?;
            let r = compile_expr(ctx,right)?;
            match op {
                &Operator::Eq => Ok(Box::new(move |row| Value::Boolean(l(row) == r(row)))),
                &Operator::NotEq => Ok(Box::new(move |row| Value::Boolean(l(row) != r(row)))),
                &Operator::Lt => Ok(Box::new(move |row| Value::Boolean(l(row) < r(row)))),
                &Operator::LtEq => Ok(Box::new(move |row| Value::Boolean(l(row) <= r(row)))),
                &Operator::Gt => Ok(Box::new(move |row| Value::Boolean(l(row) > r(row)))),
                &Operator::GtEq => Ok(Box::new(move |row| Value::Boolean(l(row) >= r(row)))),
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

            Ok(Box::new(move |row| {

                let arg_values = compiled_args_ok.iter()
                    .map(|a| a(row))
                    .collect();

                match func.execute(arg_values) {
                    Ok(value) => value,
                    Err(e) => panic!("Function returned error {:?}", e)
                }

            }))
        }
    }
}

/// Represents a csv file with a known schema
#[derive(Debug)]
pub struct CsvRelation {
    file: File,
    schema: Schema
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

impl<'a> CsvRelation {

    pub fn open(file: File, schema: Schema) -> Result<Self,ExecutionError> {
        Ok(CsvRelation { file, schema })
    }

    /// Convert StringRecord into our internal tuple type based on the known schema
    fn create_tuple(&self, r: &StringRecord) -> Result<Row,ExecutionError> {
        assert_eq!(self.schema.columns.len(), r.len());
        let values = self.schema.columns.iter().zip(r.into_iter()).map(|(c,s)| match c.data_type {
            //TODO: remove unwrap use here
            DataType::UnsignedLong => Value::Long(s.parse::<i64>().unwrap()),
            DataType::String => Value::String(s.to_string()),
            DataType::Double => Value::Double(s.parse::<f64>().unwrap()),
            _ => panic!("csv unsupported type")
        }).collect();
        Ok(Row::new(values))
    }
}

/// trait for all relations (a relation is essentially just an iterator over tuples with
/// a known schema)
pub trait SimpleRelation {
    /// scan all records in this relation
    fn scan<'a>(&'a self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Row,ExecutionError>> + 'a>;
    /// get the schema for this relation
    fn schema<'a>(&'a self) -> &'a Schema;
}

impl SimpleRelation for CsvRelation {

    fn scan<'a>(&'a self, _ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Row,ExecutionError>> + 'a> {

        let buf_reader = BufReader::new(&self.file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        let record_iter = csv_reader.into_records();

        let tuple_iter = record_iter.map(move|r| match r {
            Ok(record) => self.create_tuple(&record),
            Err(e) => Err(ExecutionError::CsvError(e))
        });

        Box::new(tuple_iter)
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }

}

impl SimpleRelation for FilterRelation {

    fn scan<'a>(&'a self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Row, ExecutionError>> + 'a> {
        Box::new(self.input.scan(ctx).filter(move|t|
            match t {
                &Ok(ref row) => match (*self.expr)(row) {
                    Value::Boolean(b) => b,
                    _ => panic!("Predicate expression evaluated to non-boolean value")
                },
                _ => true // let errors through the filter so they can be handled later
            }
        ))
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}

impl SimpleRelation for SortRelation {

    fn scan<'a>(&'a self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Row, ExecutionError>> + 'a> {

        // collect all rows from next relation
        let it = self.input.scan(ctx);

        let mut v : Vec<Row> = vec![];
        it.for_each(|item| v.push(item.unwrap()));

        // now sort them
        v.sort_by(|a,b| {

            for i in 0 .. self.sort_expr.len() {

                let evaluate = &self.sort_expr[i];
                let asc = self.sort_asc[i];

                let a_value = evaluate(a);
                let b_value = evaluate(b);

                if a_value < b_value {
                    return if asc { Less } else { Greater };
                } else if a_value > b_value {
                    return if asc { Greater } else { Less };
                }
            }

            Equal
        });

        // now return an iterator
        Box::new(v.into_iter().map(|r|Ok(r)))
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}

impl SimpleRelation for ProjectRelation {

    fn scan<'a>(&'a self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Row, ExecutionError>> + 'a> {
        let foo = self.input.scan(ctx).map(move|r| match r {
            Ok(row) => {
                let values = self.expr.iter()
                    .map(|e| (*e)(&row))
                    .collect();
                Ok(Row::new(values))
            },
            Err(_) => r
        });

        Box::new(foo)
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}

impl SimpleRelation for LimitRelation {
    fn scan<'a>(&'a self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Row, ExecutionError>> + 'a> {
        Box::new(self.input.scan(ctx).take(self.limit))
    }

    fn schema<'a>(&'a self) -> &'a Schema {
        &self.schema
    }
}

/// Execution plans are sent to worker nodes for execution
#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum ExecutionPlan {
    /// Run a query and return the results to the client
    Interactive { plan: Box<LogicalPlan> },
    /// Partition the relation
    Partition { plan: Box<LogicalPlan>, partition_count: usize, partition_expr: Expr }

}


#[derive(Debug,Clone)]
pub struct ExecutionContext {
    schemas: HashMap<String, Schema>,
    functions: HashMap<String, FunctionMeta>,
    data_dir: String

}

impl ExecutionContext {

    pub fn new(data_dir: String) -> Self {
        ExecutionContext {
            schemas: HashMap::new(),
            functions: HashMap::new(),
            data_dir
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
                Ok(Box::new(DF { ctx: Box::new(self.clone()), plan: Box::new(LogicalPlan::EmptyRelation) })) //TODO: don't clone context


            },
            _ => {
                // create a query planner
                let query_planner = SqlToRel::new(self.schemas.clone()); //TODO: pass reference to schemas

                // plan the query (create a logical relational plan)
                let plan = query_planner.sql_to_rel(&ast)?;

                // return the DataFrame
                Ok(Box::new(DF { ctx: Box::new(self.clone()), plan: plan })) //TODO: don't clone context
            }
        }


    }

    /// Open a CSV file
    ///TODO: this is building a relational plan not an execution plan so shouldn't really be here
    pub fn load(&self, filename: &str, schema: &Schema) -> Result<Box<DataFrame>, ExecutionError> {
        let plan = LogicalPlan::CsvFile { filename: filename.to_string(), schema: schema.clone() };
        Ok(Box::new(DF { ctx: Box::new((*self).clone()), plan: Box::new(plan) }))
    }

    pub fn register_table(&mut self, name: String, schema: Schema) {
        self.schemas.insert(name, schema);
    }

    pub fn create_execution_plan(&self, plan: &LogicalPlan) -> Result<Box<SimpleRelation>,ExecutionError> {
        match *plan {

            LogicalPlan::EmptyRelation => {
                Err(ExecutionError::Custom(String::from("empty relation is not implemented yet")))
            },

            LogicalPlan::TableScan { ref table_name, ref schema, .. } => {
                // for now, tables are csv files
                let filename = format!("{}/{}.csv", self.data_dir, table_name);
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
                let input_rel = self.create_execution_plan(input)?;

                let rel = FilterRelation {
                    input: input_rel,
                    expr: compile_expr(&self, expr)?,
                    schema: schema.clone()
                };
                Ok(Box::new(rel))
            },

            LogicalPlan::Projection { ref expr, ref input, .. } => {
                let input_rel = self.create_execution_plan(&input)?;
                let input_schema = input_rel.schema().clone();

                //TODO: seems to be duplicate of sql_to_rel code
                let project_columns: Vec<Field> = expr.iter().map(|e| {
                    match e {
                        &Expr::TupleValue(i) => input_schema.columns[i].clone(),
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
                let input_rel = self.create_execution_plan(input)?;

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
                let input_rel = self.create_execution_plan(input)?;
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

}




pub struct DF {
    ctx: Box<ExecutionContext>,
    plan: Box<LogicalPlan>
}

impl DataFrame for DF {

    fn select(&self, expr: Vec<Expr>) -> Result<Box<DataFrame>, DataFrameError> {

        let plan = LogicalPlan::Projection {
            expr: expr,
            input: self.plan.clone(),
            schema: self.plan.schema().clone()

        };

        Ok(Box::new(DF { ctx: self.ctx.clone(), plan: Box::new(plan) }))

    }

    fn sort(&self, expr: Vec<Expr>) -> Result<Box<DataFrame>, DataFrameError> {

        let plan = LogicalPlan::Sort {
            expr: expr,
            input: self.plan.clone(),
            schema: self.plan.schema().clone()

        };

        Ok(Box::new(DF { ctx: self.ctx.clone(), plan: Box::new(plan) }))

    }

    fn filter(&self, expr: Expr) -> Result<Box<DataFrame>, DataFrameError> {

        let plan = LogicalPlan::Selection {
            expr: expr,
            input: self.plan.clone(),
            schema: self.plan.schema().clone()
        };

        Ok(Box::new(DF { ctx: self.ctx.clone(), plan: Box::new(plan) }))
    }

    fn write(&self, filename: &str) -> Result<(), DataFrameError> {
        let execution_plan = self.ctx.create_execution_plan(&self.plan)?;

        // create output file
        // println!("Writing csv to {}", filename);
        let mut file = File::create(filename)?;

        // implement execution here for now but should be a common method for processing a plan
        let it = execution_plan.scan(&self.ctx);
        it.for_each(|t| {
            match t {
                Ok(tuple) => {
                    let csv = format!("{}\n", tuple.to_string());
                    file.write(&csv.into_bytes()).unwrap(); //TODO: remove unwrap
                },
                Err(e) => panic!(format!("Error processing tuple: {:?}", e)) //TODO: error handling
            }
        });

        Ok(())
    }

    fn col(&self, column_name: &str) -> Result<Expr, DataFrameError> {
        match self.plan.schema().column(column_name) {
            Some((i,_)) => Ok(Expr::TupleValue(i)),
            _ => Err(DataFrameError::InvalidColumn(column_name.to_string()))
        }
    }

    fn schema(&self) -> Schema {
        self.plan.schema().clone()
    }

    fn repartition(&self, _n: u32) -> Result<Box<DataFrame>, DataFrameError> {
        unimplemented!()
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

        df.write("_sqrt_out.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    #[test]
    fn test_sql_udf_udt() {

        let mut ctx = create_context();

        ctx.define_function(&STPointFunc {});

        let df = ctx.sql(&"SELECT ST_Point(lat, lng) FROM uk_cities").unwrap();

        df.write("_uk_cities_sql.csv").unwrap();

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

        df2.write("_uk_cities_df.csv").unwrap();

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
            Expr::Sort { expr: Box::new(Expr::TupleValue(1)), asc: true },
            Expr::Sort { expr: Box::new(Expr::TupleValue(2)), asc: true }
        ]).unwrap();

        df2.write("_uk_cities_sorted_by_lat_lng.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    #[test]
    fn test_chaining_functions() {

        let mut ctx = create_context();

        ctx.define_function(&STPointFunc {});

        let df = ctx.sql(&"SELECT ST_AsText(ST_Point(lat, lng)) FROM uk_cities").unwrap();

        df.write("_uk_cities_wkt.csv").unwrap();

        //TODO: check that generated file has expected contents
    }

    fn create_context() -> ExecutionContext {

        // create execution context
        let mut ctx = ExecutionContext::new("./test/data".to_string());

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
