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

use super::types::*;

use arrow::datatypes::*;

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
                let s: Vec<String> = v.iter().map(|v| v.to_string()).collect();
                s.join(",")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FunctionMeta {
    pub name: String,
    pub args: Vec<Field>,
    pub return_type: DataType,
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
        let value_strings: Vec<String> = self.iter().map(|v| v.to_string()).collect();

        // return comma-separated
        value_strings.join(",")
    }
}

#[derive(Debug, Clone)]
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
    Modulus,
}

/// Relation Expression
#[derive(Debug, Clone)]
pub enum Expr {
    /// index into a value within the row or complex value
    Column(usize),
    /// literal value
    Literal(ScalarValue),
    /// binary expression e.g. "age > 21"
    BinaryExpr {
        left: Rc<Expr>,
        op: Operator,
        right: Rc<Expr>,
    },
    /// sort expression
    Sort { expr: Rc<Expr>, asc: bool },
    /// scalar function
    ScalarFunction { name: String, args: Vec<Expr> },
}

impl Expr {
    pub fn eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::Eq,
            right: Rc::new(other.clone()),
        }
    }

    pub fn gt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::Gt,
            right: Rc::new(other.clone()),
        }
    }

    pub fn lt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::Lt,
            right: Rc::new(other.clone()),
        }
    }
}

/// Relations
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Limit {
        limit: usize,
        input: Rc<LogicalPlan>,
        schema: Rc<Schema>,
    },
    Projection {
        expr: Vec<Expr>,
        input: Rc<LogicalPlan>,
        schema: Rc<Schema>,
    },
    Selection {
        expr: Expr,
        input: Rc<LogicalPlan>,
        schema: Rc<Schema>,
    },
    Sort {
        expr: Vec<Expr>,
        input: Rc<LogicalPlan>,
        schema: Rc<Schema>,
    },
    TableScan {
        schema_name: String,
        table_name: String,
        schema: Rc<Schema>,
    },
    CsvFile {
        filename: String,
        schema: Rc<Schema>,
    },
    ParquetFile {
        filename: String,
        schema: Rc<Schema>,
    },
    EmptyRelation {
        schema: Rc<Schema>,
    },
}

impl LogicalPlan {
    pub fn schema(&self) -> &Rc<Schema> {
        match self {
            LogicalPlan::EmptyRelation { schema } => schema,
            LogicalPlan::TableScan { schema, .. } => schema,
            LogicalPlan::CsvFile { schema, .. } => schema,
            LogicalPlan::ParquetFile { schema, .. } => schema,
            LogicalPlan::Projection { schema, .. } => schema,
            LogicalPlan::Selection { schema, .. } => schema,
            LogicalPlan::Sort { schema, .. } => schema,
            LogicalPlan::Limit { schema, .. } => schema,
        }
    }
}

//#[cfg(test)]
//mod tests {
//
//    use super::Expr::*;
//    use super::LogicalPlan::*;
//    use super::ScalarValue::*;
//    use super::*;
//
//    #[test]
//    fn serde() {
//        let schema = Schema {
//            columns: vec![
//                Field {
//                    name: "id".to_string(),
//                    data_type: DataType::Int32,
//                    nullable: false,
//                },
//                Field {
//                    name: "name".to_string(),
//                    data_type: DataType::Utf8,
//                    nullable: false,
//                },
//            ],
//        };
//
//        let csv = CsvFile {
//            filename: "test/data/people.csv".to_string(),
//            schema: schema.clone(),
//        };
//
//        let filter_expr = BinaryExpr {
//            left: Rc::new(Column(0)),
//            op: Operator::Eq,
//            right: Rc::new(Literal(Int64(2))),
//        };
//
//        let _ = Selection {
//            expr: filter_expr,
//            input: Rc::new(csv),
//            schema: schema.clone(),
//        };
//
//        //        let s = serde_json::to_string(&plan).unwrap();
//        //        println!("serialized: {}", s);
//    }
//
//}
