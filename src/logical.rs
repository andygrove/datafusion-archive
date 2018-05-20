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
    name: String,
    args: Vec<Field>,
    return_type: DataType,
    function_type: FunctionType,
}

impl FunctionMeta {
    pub fn new(
        name: String,
        args: Vec<Field>,
        return_type: DataType,
        function_type: FunctionType,
    ) -> Self {
        FunctionMeta {
            name,
            args,
            return_type,
            function_type,
        }
    }
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn args(&self) -> &Vec<Field> {
        &self.args
    }
    pub fn return_type(&self) -> &DataType {
        &self.return_type
    }
    pub fn function_type(&self) -> &FunctionType {
        &self.function_type
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub fn get_datatype(&self, l: &Expr, _r: &Expr, schema: &Schema) -> DataType {
        //TODO: implement correctly, just go with left side for now
        l.get_type(schema).clone()
    }
}

/// Relation Expression
#[derive(Clone, PartialEq)]
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
            Expr::AggregateFunction { return_type, .. } => return_type.clone(),
            Expr::BinaryExpr {
                ref left,
                ref right,
                ref op,
            } => {
                match op {
                    Operator::Eq | Operator::NotEq => DataType::Boolean,
                    Operator::Lt | Operator::LtEq => DataType::Boolean,
                    Operator::Gt | Operator::GtEq => DataType::Boolean,
                    Operator::And | Operator::Or => DataType::Boolean,
                    _ => {
                        let left_type = left.get_type(schema);
                        let right_type = right.get_type(schema);
                        get_supertype(&left_type, &right_type).unwrap_or(DataType::Utf8) //TODO ???
                    }
                }
            }
            Expr::Sort { ref expr, .. } => expr.get_type(schema),
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
            Expr::Cast { expr, data_type } => write!(f, "CAST({:?} AS {:?})", expr, data_type),
            Expr::BinaryExpr { left, op, right } => write!(f, "{:?} {:?} {:?}", left, op, right),
            Expr::Sort { expr, asc } => if *asc {
                write!(f, "{:?} ASC", expr)
            } else {
                write!(f, "{:?} DESC", expr)
            },
            Expr::ScalarFunction { name, ref args, .. } => {
                write!(f, "{}(", name)?;
                for i in 0..args.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i])?;
                }

                write!(f, ")")
            }
            Expr::AggregateFunction { name, ref args, .. } => {
                write!(f, "{}(", name)?;
                for i in 0..args.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i])?;
                }

                write!(f, ")")
            }
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
    /// Represents an ndjson file with a provided schema
    NdJsonFile {
        filename: String,
        schema: Rc<Schema>,
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
            LogicalPlan::NdJsonFile { schema, .. } => &schema,
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
        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "  ")?;
            }
        }
        match *self {
            LogicalPlan::EmptyRelation { .. } => write!(f, "EmptyRelation"),
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
            LogicalPlan::NdJsonFile {
                ref filename,
                ref schema,
                ..
            } => write!(f, "NdJsonFile: file={}, schema={:?}", filename, schema),
            LogicalPlan::ParquetFile { .. } => write!(f, "ParquetFile:"),
            LogicalPlan::Projection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Projection: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
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
            LogicalPlan::Sort {
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Sort: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Limit {
                ref input, limit, ..
            } => {
                write!(f, "Limit: {}", limit)?;
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
