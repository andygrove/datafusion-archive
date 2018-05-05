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

//! Logical plan

use std::fmt;
use std::fmt::{Error, Formatter};
use std::rc::Rc;

use super::types::*;

use arrow::datatypes::*;

#[derive(Debug, Clone)]
pub enum FunctionType {
    Scalar,
    Aggregate,
}

#[derive(Debug, Clone)]
pub struct FunctionMeta {
    pub name: String,
    pub args: Vec<Field>,
    pub return_type: DataType,
    pub function_type: FunctionType,
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
    And,
    Or,
}

impl Operator {
    /// Get the result type of applying this operation to its left and right inputs
    pub fn get_datatype(&self, l: &Expr, _r:&Expr, schema: &Schema) -> DataType {
        //TODO: implement correctly, just go with left side for now
        l.get_type(schema).clone()
    }
}

/// Relation Expression
#[derive(Clone)]
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
    /// cast a value to a different type
    Cast { expr: Rc<Expr>, data_type: DataType },
    /// sort expression
    Sort { expr: Rc<Expr>, asc: bool },
    /// scalar function
    ScalarFunction {
        name: String,
        args: Vec<Expr>,
        return_type: DataType,
    },
    /// aggregate function
    AggregateFunction {
        name: String,
        args: Vec<Expr>,
        return_type: DataType,
    },
}

impl Expr {
    pub fn get_type(&self, schema: &Schema) -> DataType {
        match self {
            Expr::Column(n) => schema.column(*n).data_type().clone(),
            Expr::Literal(l) => l.get_datatype(),
            Expr::Cast { data_type, .. } => data_type.clone(),
            Expr::ScalarFunction { return_type, .. } => return_type.clone(),
            _ => unimplemented!(),
        }
    }

    pub fn cast_to(&self, cast_to_type: &DataType, schema: &Schema) -> Result<Expr, String> {
        let this_type = self.get_type(schema);
        if this_type == *cast_to_type {
            Ok(self.clone())
        } else if cast_to_type.can_coerce_from(&this_type) {
            Ok(Expr::Cast {
                expr: Rc::new(self.clone()),
                data_type: cast_to_type.clone(),
            })
        } else {
            Err(format!(
                "Cannot automatically convert {:?} to {:?}",
                this_type, cast_to_type
            ))
        }
    }

    pub fn eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::Eq,
            right: Rc::new(other.clone()),
        }
    }

    pub fn not_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::NotEq,
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

    pub fn gt_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::GtEq,
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

    pub fn lt_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::LtEq,
            right: Rc::new(other.clone()),
        }
    }
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            Expr::Column(i) => write!(f, "#{}", i),
            Expr::Literal(v) => write!(f, "{:?}", v),
            Expr::Cast { expr, data_type } => write!(f, "CAST {:?} AS {:?}", expr, data_type),
            Expr::BinaryExpr { left, op, right } => write!(f, "{:?} {:?} {:?}", left, op, right),
            Expr::Sort { expr, asc } => if *asc {
                write!(f, "{:?} ASC", expr)
            } else {
                write!(f, "{:?} DESC", expr)
            },
            Expr::ScalarFunction { name, .. } => write!(f, "{}()", name),
            Expr::AggregateFunction { name, .. } => write!(f, "{}()", name),
        }
    }
}

/// The LogicalPlan represents different types of relations (such as Projection, Selection, etc) and
/// can be created by the SQL query planner and the DataFrame API.
#[derive(Clone)]
pub enum LogicalPlan {
    /// A relation that applies a row limit to its child relation
    Limit {
        limit: usize,
        input: Rc<LogicalPlan>,
        schema: Rc<Schema>,
    },
    /// A Projection (essentially a SELECT with an expression list)
    Projection {
        expr: Vec<Expr>,
        input: Rc<LogicalPlan>,
        schema: Rc<Schema>,
    },
    /// A Selection (essentially a WHERE clause with a predicate expression)
    Selection { expr: Expr, input: Rc<LogicalPlan> },
    /// Represents a list of aggregate expressions with optional grouping expressions
    Aggregate {
        input: Rc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        schema: Rc<Schema>,
    },
    /// Represents a list of sort expressions to be applied to a relation
    Sort {
        expr: Vec<Expr>,
        input: Rc<LogicalPlan>,
        schema: Rc<Schema>,
    },
    /// A table scan against a table that has been registered on a context
    TableScan {
        schema_name: String,
        table_name: String,
        schema: Rc<Schema>,
        projection: Option<Vec<usize>>,
    },
    /// Represents a CSV file with a provided schema
    CsvFile {
        filename: String,
        schema: Rc<Schema>,
        has_header: bool,
        projection: Option<Vec<usize>>,
    },
    /// Represents a Parquet file that contains schema information
    ParquetFile {
        filename: String,
        schema: Rc<Schema>,
        projection: Option<Vec<usize>>,
    },
    /// An empty relation with an empty schema
    EmptyRelation { schema: Rc<Schema> },
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &Rc<Schema> {
        match self {
            LogicalPlan::EmptyRelation { schema } => &schema,
            LogicalPlan::TableScan { schema, .. } => &schema,
            LogicalPlan::CsvFile { schema, .. } => &schema,
            LogicalPlan::ParquetFile { schema, .. } => &schema,
            LogicalPlan::Projection { schema, .. } => &schema,
            LogicalPlan::Selection { input, .. } => input.schema(),
            LogicalPlan::Aggregate { schema, .. } => &schema,
            LogicalPlan::Sort { schema, .. } => &schema,
            LogicalPlan::Limit { schema, .. } => &schema,
        }
    }
}

impl LogicalPlan {
    fn fmt_with_indent(&self, f: &mut Formatter, indent: usize) -> Result<(), Error> {
        write!(f, "\n")?;
        for _ in 0..indent {
            write!(f, "  ")?;
        }
        match *self {
            LogicalPlan::EmptyRelation { .. } => write!(f, "EmptyRelation:"),
            LogicalPlan::TableScan {
                ref table_name,
                ref projection,
                ..
            } => write!(f, "TableScan: {} projection={:?}", table_name, projection),
            LogicalPlan::CsvFile {
                ref filename,
                ref schema,
                ..
            } => write!(f, "CsvFile: file={}, schema={:?}", filename, schema),
            LogicalPlan::ParquetFile { .. } => write!(f, "ParquetFile:"),
            LogicalPlan::Projection { ref input, .. } => {
                write!(f, "Projection:")?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Selection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Selection: {:?}", expr)?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Aggregate {
                ref input,
                ref group_expr,
                ref aggr_expr,
                ..
            } => {
                write!(
                    f,
                    "Aggregate: groupBy=[{:?}], aggr=[{:?}]",
                    group_expr, aggr_expr
                )?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Sort { ref input, .. } => {
                write!(f, "Sort:")?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Limit { ref input, .. } => {
                write!(f, "Limit:")?;
                input.fmt_with_indent(f, indent + 1)
            }
        }
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        self.fmt_with_indent(f, 0)
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
