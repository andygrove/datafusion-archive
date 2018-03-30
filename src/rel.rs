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

use std::cmp::Ordering;

extern crate arrow;
use self::arrow::datatypes::*;

/// Value holder for all supported data types
#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub enum ScalarValue {
    Boolean(bool),
    Float32(f32),
    Float64(f64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Utf8(String),
    Struct(Vec<ScalarValue>),
}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &ScalarValue) -> Option<Ordering> {

        //TODO: implement all type coercion rules

        match self {
            &ScalarValue::Float64(l) => match other {
                &ScalarValue::Float64(r) => l.partial_cmp(&r),
                &ScalarValue::Int64(r) => l.partial_cmp(&(r as f64)),
                _ => unimplemented!("type coercion rules missing")
            },
            &ScalarValue::Int64(l) => match other {
                &ScalarValue::Float64(r) => (l as f64).partial_cmp(&r),
                &ScalarValue::Int64(r) => l.partial_cmp(&r),
                _ => unimplemented!("type coercion rules missing")
            },
            &ScalarValue::Utf8(ref l) => match other {
                &ScalarValue::Utf8(ref r) => l.partial_cmp(r),
                _ => unimplemented!("type coercion rules missing")
            },
            &ScalarValue::Struct(_) => None,
            _ => unimplemented!("type coercion rules missing")
        }

    }
}


impl ScalarValue {

    pub fn to_string(&self) -> String {
        match self {
            &ScalarValue::Boolean(b) => b.to_string(),
            &ScalarValue::Int8(l) => l.to_string(),
            &ScalarValue::Int16(l) => l.to_string(),
            &ScalarValue::Int32(l) => l.to_string(),
            &ScalarValue::Int64(l) => l.to_string(),
            &ScalarValue::UInt8(l) => l.to_string(),
            &ScalarValue::UInt16(l) => l.to_string(),
            &ScalarValue::UInt32(l) => l.to_string(),
            &ScalarValue::UInt64(l) => l.to_string(),
            &ScalarValue::Float32(d) => d.to_string(),
            &ScalarValue::Float64(d) => d.to_string(),
            &ScalarValue::Utf8(ref s) => s.clone(),
            &ScalarValue::Struct(ref v) => {
                let s : Vec<String> = v.iter()
                    .map(|v| v.to_string())
                    .collect();
                s.join(",")
            }
        }
    }

}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct FunctionMeta {
    pub name: String,
    pub args: Vec<Field>,
    pub return_type: DataType
}

pub trait Row {
    fn get(&self, index: usize) -> &ScalarValue;
    fn to_string(&self) -> String;
}

impl Row for Vec<ScalarValue> {

    fn get(&self, index: usize) -> &ScalarValue {
        &self[index]
    }

    fn to_string(&self) -> String {
        let value_strings : Vec<String> = self.iter()
            .map(|v| v.to_string())
            .collect();

        // return comma-separated
        value_strings.join(",")
    }
}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulus
}

/// Relation Expression
#[derive(Debug,Clone,Serialize, Deserialize)]
pub enum Expr {
    /// index into a value within the row or complex value
    Column(usize),
    /// literal value
    Literal(ScalarValue),
    /// binary expression e.g. "age > 21"
    BinaryExpr { left: Box<Expr>, op: Operator, right: Box<Expr> },
    /// sort expression
    Sort { expr: Box<Expr>, asc: bool },
    /// scalar function
    ScalarFunction { name: String, args: Vec<Expr> }
}

impl Expr {

    pub fn eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::Eq,
            right: Box::new(other.clone())
        }
    }

    pub fn gt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::Gt,
            right: Box::new(other.clone())
        }
    }

    pub fn lt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::Lt,
            right: Box::new(other.clone())
        }
    }

}

/// Relations
#[derive(Debug,Clone,Serialize, Deserialize)]
pub enum LogicalPlan {
    Limit { limit: usize, input: Box<LogicalPlan>, schema: Schema },
    Projection { expr: Vec<Expr>, input: Box<LogicalPlan>, schema: Schema },
    Selection { expr: Expr, input: Box<LogicalPlan>, schema: Schema },
    Sort { expr: Vec<Expr>, input: Box<LogicalPlan>, schema: Schema },
    TableScan { schema_name: String, table_name: String, schema: Schema },
    CsvFile { filename: String, schema: Schema },
    EmptyRelation
}

impl LogicalPlan {

    pub fn schema(&self) -> Schema {
        match self {
            &LogicalPlan::EmptyRelation => Schema::empty(),
            &LogicalPlan::TableScan { ref schema, .. } => schema.clone(),
            &LogicalPlan::CsvFile { ref schema, .. } => schema.clone(),
            &LogicalPlan::Projection { ref schema, .. } => schema.clone(),
            &LogicalPlan::Selection { ref schema, .. } => schema.clone(),
            &LogicalPlan::Sort { ref schema, .. } => schema.clone(),
            &LogicalPlan::Limit { ref schema, .. } => schema.clone(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use super::LogicalPlan::*;
    use super::Expr::*;
    use super::ScalarValue::*;
    extern crate serde_json;

    #[test]
    fn serde() {

        let schema = Schema {
            columns: vec![
                Field { name: "id".to_string(), data_type: DataType::Int32, nullable: false },
                Field { name: "name".to_string(), data_type: DataType::Utf8, nullable: false }
            ]
        };

        let csv = CsvFile { filename: "test/data/people.csv".to_string(), schema: schema.clone() };

        let filter_expr = BinaryExpr {
            left: Box::new(Column(0)),
            op: Operator::Eq,
            right: Box::new(Literal(Int64(2)))
        };

        let plan = Selection {
            expr: filter_expr,
            input: Box::new(csv),
            schema: schema.clone()

        };

        let s = serde_json::to_string(&plan).unwrap();
        println!("serialized: {}", s);
    }

}

