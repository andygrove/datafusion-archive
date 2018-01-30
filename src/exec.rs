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

use std::collections::HashMap;
use std::io::{Error, ErrorKind, Read};
use std::io::{BufReader, BufRead};
use std::io::prelude::*;
use std::iter::Iterator;
use std::fs::File;
use std::path::Path;
use std::string::String;
use std::convert::*;

extern crate csv;

use super::csv::StringRecord;

use super::rel::*;
use super::dataframe::*;

#[derive(Debug)]
pub enum ExecutionError {
    IoError(Error),
    CsvError(csv::Error),
    Custom(String)
}

impl From<Error> for ExecutionError {
    fn from(e: Error) -> Self {
        ExecutionError::IoError(e)
    }
}

/// Represents a csv file with a known schema
#[derive(Debug)]
pub struct CsvRelation {
    file: File,
    schema: TupleType
}

pub struct FilterRelation {
    schema: TupleType,
    input: Box<SimpleRelation>,
    expr: Rex
}

pub struct ProjectRelation {
    schema: TupleType,
    input: Box<SimpleRelation>,
    expr: Vec<Rex>
}

impl<'a> CsvRelation {

    pub fn open(file: File, schema: TupleType) -> Result<Self,ExecutionError> {
        Ok(CsvRelation { file, schema })
    }

    /// Convert StringRecord into our internal tuple type based on the known schema
    fn create_tuple(&self, r: &StringRecord) -> Result<Tuple,ExecutionError> {
        assert_eq!(self.schema.columns.len(), r.len());
        let values = self.schema.columns.iter().zip(r.into_iter()).map(|(c,s)| match c.data_type {
            //TODO: remove unwrap use here
            DataType::UnsignedLong => Value::UnsignedLong(s.parse::<u64>().unwrap()),
            DataType::String => Value::String(s.to_string()),
            DataType::Double => Value::Double(s.parse::<f64>().unwrap()),
        }).collect();
        Ok(Tuple::new(values))
    }
}

/// trait for all relations (a relation is essentially just an iterator over tuples with
/// a known schema)
pub trait SimpleRelation {
    /// scan all records in this relation
    fn scan<'a>(&'a self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Tuple,ExecutionError>> + 'a>;
    /// get the schema for this relation
    fn schema<'a>(&'a self) -> &'a TupleType;
}

impl SimpleRelation for CsvRelation {

    fn scan<'a>(&'a self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Tuple,ExecutionError>> + 'a> {

        let buf_reader = BufReader::new(&self.file);
        let csv_reader = csv::Reader::from_reader(buf_reader);
        let record_iter = csv_reader.into_records();

        let tuple_iter = record_iter.map(move|r| match r {
            Ok(record) => self.create_tuple(&record),
            Err(e) => Err(ExecutionError::CsvError(e))
        });

        Box::new(tuple_iter)
    }

    fn schema<'a>(&'a self) -> &'a TupleType {
        &self.schema
    }

}

impl SimpleRelation for FilterRelation {

    fn scan<'a>(&'a self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Tuple, ExecutionError>> + 'a> {
        Box::new(self.input.scan(ctx).filter(move|t|
            match t {
                &Ok(ref tuple) => match ctx.evaluate(tuple, &self.schema, &self.expr) {
                    Ok(Value::Boolean(b)) => b,
                    _ => panic!("Predicate expression evaluated to non-boolean value")
                },
                _ => true // let errors through the filter so they can be handled later
            }
        ))
    }

    fn schema<'a>(&'a self) -> &'a TupleType {
        &self.schema
    }
}

impl SimpleRelation for ProjectRelation {

    fn scan<'a>(&'a self, ctx: &'a ExecutionContext) -> Box<Iterator<Item=Result<Tuple, ExecutionError>> + 'a> {
        let foo = self.input.scan(ctx).map(move|r| match r {
            Ok(tuple) => {
                let values = self.expr.iter()
                    .map(|e| match e {
                        &Rex::TupleValue(i) => tuple.values[i].clone(),
                        //TODO: relation delegating back to execution context seems wrong way around
                        _ => ctx.evaluate(&tuple,&self.schema, e).unwrap() //TODO: remove unwrap
                        //unimplemented!("Unsupported expression for projection")
                    })
                    .collect();
                Ok(Tuple::new(values))
            },
            Err(_) => r
        });

        Box::new(foo)
    }

    fn schema<'a>(&'a self) -> &'a TupleType {
        &self.schema
    }
}

pub trait ScalarFunction {
    fn execute(args: Vec<Value>) -> Value;

}

#[derive(Debug,Clone)]
pub struct ExecutionContext {
    schemas: HashMap<String, TupleType>,
    functions: HashMap<String, FunctionMeta>,

}

impl ExecutionContext {

    pub fn new(schemas: HashMap<String, TupleType>) -> Self {
        ExecutionContext { schemas: schemas, functions: HashMap::new() }
    }

    pub fn define_function(&mut self, fm: FunctionMeta) {
        self.functions.insert(fm.name.to_lowercase(), fm);
    }

    /// Open a CSV file
    ///TODO: this is building a relational plan not an execution plan so shouldn't really be here
    pub fn load(&self, filename: &str, schema: &TupleType) -> Result<Box<DataFrame>, ExecutionError> {
        let plan = Rel::CsvFile { filename: filename.to_string(), schema: schema.clone() };
        Ok(Box::new(DF { ctx: Box::new((*self).clone()), plan: Box::new(plan) }))
    }

    pub fn register_table(&mut self, name: String, schema: TupleType) {
        self.schemas.insert(name, schema);
    }

    pub fn create_execution_plan(&self, plan: &Rel) -> Result<Box<SimpleRelation>,ExecutionError> {
        match *plan {

            Rel::EmptyRelation => {
                panic!()
            },

            Rel::TableScan { ref schema_name, ref table_name, ref schema } => {
                // for now, tables are csv files
                let file = File::open(format!("test/{}.csv", table_name))?;
                let rel = CsvRelation::open(file, schema.clone())?;
                Ok(Box::new(rel))
            },

            Rel::CsvFile { ref filename, ref schema } => {
                let file = File::open(filename)?;
                let rel = CsvRelation::open(file, schema.clone())?;
                Ok(Box::new(rel))
            },

            Rel::Selection { ref expr, ref input, ref schema } => {
                let input_rel = self.create_execution_plan(input)?;
                let rel = FilterRelation {
                    input: input_rel,
                    expr: expr.clone(),
                    schema: schema.clone()
                };
                Ok(Box::new(rel))
            },

            Rel::Projection { ref expr, ref input, ref schema } => {
                let input_rel = self.create_execution_plan(&input)?;
                let input_schema = input_rel.schema().clone();

                //TODO: seems to be duplicate of sql_to_rel code
                let project_columns: Vec<ColumnMeta> = expr.iter().map(|e| {
                    match e {
                        &Rex::TupleValue(i) => input_schema.columns[i].clone(),
                        &Rex::ScalarFunction {ref name, ref args} => ColumnMeta {
                            name: name.clone(),
                            data_type: DataType::Double, //TODO: hard-coded .. no function metadata yet
                            nullable: true
                        },
                        _ => unimplemented!("Unsupported projection expression")
                    }
                }).collect();

                let project_schema = TupleType { columns: project_columns };

                let rel = ProjectRelation {
                    input: input_rel,
                    expr: expr.clone(),
                    schema: project_schema,

                };

                Ok(Box::new(rel))
            }
        }
    }

    /// Evaluate a relational expression against a tuple
    pub fn evaluate(&self, tuple: &Tuple, tt: &TupleType, rex: &Rex) -> Result<Value, Box<ExecutionError>> {

        match rex {
            &Rex::BinaryExpr { ref left, ref op, ref right } => {
                let left_value = self.evaluate(tuple, tt, left)?;
                let right_value = self.evaluate(tuple, tt, right)?;
                match op {
                    &Operator::Eq => Ok(Value::Boolean(left_value == right_value)),
                    &Operator::NotEq => Ok(Value::Boolean(left_value != right_value)),
                    &Operator::Lt => Ok(Value::Boolean(left_value < right_value)),
                    &Operator::LtEq => Ok(Value::Boolean(left_value <= right_value)),
                    &Operator::Gt => Ok(Value::Boolean(left_value > right_value)),
                    &Operator::GtEq => Ok(Value::Boolean(left_value >= right_value)),
                }
            },
            &Rex::TupleValue(index) => Ok(tuple.values[index].clone()),
            &Rex::Literal(ref value) => Ok(value.clone()),
            &Rex::ScalarFunction { ref name, ref args } => {

                //TODO: look up function dynamically in execution context
                //TODO: do arg check first based on function definition (count + types)
                //TODO: function definition and implemenation should be separate things

                // evaluate the arguments to the function
                let arg_values : Vec<Value> = args.iter()
                    .map(|a| self.evaluate(tuple, tt, &a))
                    .collect::<Result<Vec<Value>, Box<ExecutionError>>>()?;

                match name.as_ref() {
                    "sqrt" => {
                        match arg_values[0] {
                            Value::Double(d) => Ok(Value::Double(d.sqrt())),
                            Value::UnsignedLong(l) => Ok(Value::Double((l as f64).sqrt())),
                            _ => Err(Box::new(ExecutionError::Custom("Unsupported arg type for sqrt".to_string())))
                        }

                    },
                    _ => Err(Box::new(ExecutionError::Custom("Unknown function".to_string())))
                }

                //unimplemented!()
            }
        }

    }

}




pub struct DF {
    ctx: Box<ExecutionContext>,
    plan: Box<Rel>
}

impl DataFrame for DF {

    fn repartition(&self, n: u32) -> Result<Box<DataFrame>, DataFrameError> {
        unimplemented!()
    }

    fn select(&self, expr: Vec<Rex>) -> Result<Box<DataFrame>, DataFrameError> {
        unimplemented!()
    }

    fn filter(&self, expr: Rex) -> Result<Box<DataFrame>, DataFrameError> {

        let plan = Rel::Selection {
            expr: expr,
            input: self.plan.clone(),
            schema: self.plan.schema().clone()
        };

        Ok(Box::new(DF { ctx: self.ctx.clone(), plan: Box::new(plan) }))
    }

    fn write(&self, filename: &str) -> Result<(), DataFrameError> {
        let execution_plan = self.ctx.create_execution_plan(&self.plan)?;

        // create output file
        let mut file = File::create(filename)?;

        // implement execution here for now but should be a common method for processing a plan
        let it = execution_plan.scan(&self.ctx);
        it.for_each(|t| {
            match t {
                Ok(tuple) => {
                    let csv = format!("{:?}", tuple);
                    file.write(&csv.into_bytes());
                },
                _ => println!("Error") //TODO: error handling
            }
        });

        Ok(())
    }

    fn col(&self, column_name: &str) -> Result<Rex, DataFrameError> {
        match &self.plan.as_ref() {
            &&Rel::CsvFile { ref filename, ref schema } => match schema.column(column_name) {
                Some((i,c)) => Ok(Rex::TupleValue(i)),
                _ => Err(DataFrameError::TBD) // column doesn't exist
            },
            _ => Err(DataFrameError::NotImplemented)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::parser::*;
    use super::super::rel::*;
    use super::super::sqltorel::*;

    #[test]
    fn test_sqrt() {

        //TODO: refactor so there is a way to write concise tests


        let sql = "SELECT id, sqrt(id) FROM people";

        // parse SQL into AST
        let ast = Parser::parse_sql(String::from(sql)).unwrap();

        // define schema for a csv file
        let schema = TupleType {
            columns: vec![
                ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
                ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
            ]
        };

        // create a schema registry
        let mut schemas : HashMap<String, TupleType> = HashMap::new();
        schemas.insert("people".to_string(), schema.clone());

        // create a query planner
        let query_planner = SqlToRel::new(schemas.clone());

        // plan the query (create a logical relational plan)
        let plan = query_planner.sql_to_rel(&ast).unwrap();

        // create execution context
        let mut ctx = ExecutionContext::new(schemas.clone());

        ctx.define_function( FunctionMeta {
            name: "sqrt".to_string(),
            args: vec![ ColumnMeta::new("value", DataType::Double, false) ],
            return_type: DataType::Double
        });

        // create execution plan
        let execution_plan = ctx.create_execution_plan(&plan).unwrap();

        // execute the query
        let it = execution_plan.scan(&ctx);
        let results : Vec<String> = it.map(|t| {
            match t {
                Ok(tuple) => tuple.to_string(),
                _ => format!("error")
            }
        })
        .collect();

        println!("Result: {:?}", results.join(","));

        let expected = "1,1,\
            2,1.4142135623730951,\
            3,1.7320508075688772,\
            4,2,\
            5,2.23606797749979,\
            6,2.449489742783178,\
            7,2.6457513110645907,\
            8,2.8284271247461903,\
            9,3,\
            10,3.1622776601683795";

        assert_eq!(expected, results.join(","));

    }
}
