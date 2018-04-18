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

//! SQL Query Planner (produces logical plan from SQL AST)

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::string::String;

use super::logical::*;
use super::sqlast::*;
use super::types::*;

use arrow::datatypes::*;

/// SQL query planner
pub struct SqlToRel {
    //default_schema: Option<String>,
    schemas: Rc<RefCell<HashMap<String, Rc<Schema>>>>,
}

impl SqlToRel {
    /// Create a new query planner
    pub fn new(schemas: Rc<RefCell<HashMap<String, Rc<Schema>>>>) -> Self {
        SqlToRel {
            /*default_schema: None,*/ schemas,
        }
    }

    /// Generate a logic plan from a SQL AST node
    pub fn sql_to_rel(&self, sql: &ASTNode) -> Result<Rc<LogicalPlan>, String> {
        match sql {
            &ASTNode::SQLSelect {
                ref projection,
                ref relation,
                ref selection,
                ref limit,
                ref order_by,
                ref group_by,
                ref having,
                ..
            } => {

                // parse the input relation so we have access to the row type
                let input = match relation {
                    &Some(ref r) => self.sql_to_rel(r)?,
                    &None => Rc::new(LogicalPlan::EmptyRelation {
                        schema: Rc::new(Schema::empty()),
                    }),
                };

                let input_schema = input.schema();

                let expr: Vec<Expr> = projection
                    .iter()
                    .map(|e| self.sql_to_rex(&e, &input_schema))
                    .collect::<Result<Vec<Expr>, String>>()?;

                // collect aggregate expressions
                let aggr_expr: Vec<Expr> = expr.iter().filter(|e| match e {
                    Expr::AggregateFunction { .. } => true,
                    _ => false
                }).map(|e| e.clone()).collect();

                if aggr_expr.len() > 0 {
                    let aggr_schema = Schema::new(expr_to_field(&aggr_expr, input_schema));
                    Ok(Rc::new(LogicalPlan::Aggregate {
                        input: input.clone(),
                        group_expr: Vec::new(),
                        aggr_expr: aggr_expr,
                        schema: Rc::new(aggr_schema),
                    }))
                } else {
                    let projection_schema = Rc::new(Schema {
                        columns: expr_to_field(&expr, input_schema.as_ref())
                    });

                    let selection_plan = match selection {
                        &Some(ref filter_expr) => {
                            let selection_rel = LogicalPlan::Selection {
                                expr: self.sql_to_rex(&filter_expr, &input_schema.clone())?,
                                input: input.clone(),
                                schema: input_schema.clone(),
                            };

                            LogicalPlan::Projection {
                                expr: expr,
                                input: Rc::new(selection_rel),
                                schema: projection_schema.clone(),
                            }
                        }
                        _ => LogicalPlan::Projection {
                            expr: expr,
                            input: input.clone(),
                            schema: projection_schema.clone(),
                        },
                    };

                    // aggregate queries
//                    match group_by {
//                        Some(g) => Err(String::from("GROUP BY is not implemented yet")),
//                        None => {}
//                    }

                    if let &Some(_) = having {
                        return Err(String::from("HAVING is not implemented yet"));
                    }

                    let order_by_plan = match order_by {
                        &Some(ref order_by_expr) => {
                            let input_schema = selection_plan.schema();
                            let order_by_rex: Result<Vec<Expr>, String> = order_by_expr
                                .iter()
                                .map(|e| self.sql_to_rex(e, &input_schema))
                                .collect();

                            LogicalPlan::Sort {
                                expr: order_by_rex?,
                                input: Rc::new(selection_plan.clone()),
                                schema: input_schema.clone(),
                            }
                        }
                        _ => selection_plan,
                    };

                    let limit_plan = match limit {
                        &Some(ref limit_ast_node) => {
                            let limit_count = match **limit_ast_node {
                                ASTNode::SQLLiteralLong(n) => n,
                                _ => return Err(String::from("LIMIT parameter is not a number")),
                            };
                            LogicalPlan::Limit {
                                limit: limit_count as usize,
                                schema: order_by_plan.schema().clone(),
                                input: Rc::new(order_by_plan),
                            }
                        }
                        _ => order_by_plan,
                    };

                    Ok(Rc::new(limit_plan))
                }
            }

            &ASTNode::SQLIdentifier(ref id) => match self.schemas.borrow().get(id) {
                Some(schema) => Ok(Rc::new(LogicalPlan::TableScan {
                    schema_name: String::from("default"),
                    table_name: id.clone(),
                    schema: schema.clone(),
                })),
                None => Err(format!("no schema found for table {}", id)),
            },

            _ => Err(format!(
                "sql_to_rel does not support this relation: {:?}",
                sql
            )),
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(&self, sql: &ASTNode, schema: &Schema) -> Result<Expr, String> {
        match sql {
            &ASTNode::SQLLiteralLong(n) => Ok(Expr::Literal(ScalarValue::Int64(n))),
            &ASTNode::SQLLiteralDouble(n) => Ok(Expr::Literal(ScalarValue::Float64(n))),

            &ASTNode::SQLIdentifier(ref id) => {
                match schema.columns.iter().position(|c| c.name.eq(id)) {
                    Some(index) => Ok(Expr::Column(index)),
                    None => Err(format!(
                        "Invalid identifier '{}' for schema {}",
                        id,
                        schema.to_string()
                    )),
                }
            }

            &ASTNode::SQLBinaryExpr {
                ref left,
                ref op,
                ref right,
            } => {
                //TODO: we have this implemented somewhere else already
                let operator = match op {
                    &SQLOperator::Gt => Operator::Gt,
                    &SQLOperator::GtEq => Operator::GtEq,
                    &SQLOperator::Lt => Operator::Lt,
                    &SQLOperator::LtEq => Operator::LtEq,
                    &SQLOperator::Eq => Operator::Eq,
                    &SQLOperator::NotEq => Operator::NotEq,
                    &SQLOperator::Plus => Operator::Plus,
                    &SQLOperator::Minus => Operator::Minus,
                    &SQLOperator::Multiply => Operator::Multiply,
                    &SQLOperator::Divide => Operator::Divide,
                    &SQLOperator::Modulus => Operator::Modulus,
                };
                Ok(Expr::BinaryExpr {
                    left: Rc::new(self.sql_to_rex(&left, &schema)?),
                    op: operator,
                    right: Rc::new(self.sql_to_rex(&right, &schema)?),
                })
            }

            &ASTNode::SQLOrderBy { ref expr, asc } => Ok(Expr::Sort {
                expr: Rc::new(self.sql_to_rex(&expr, &schema)?),
                asc,
            }),

            &ASTNode::SQLFunction { ref id, ref args } => {
                let rex_args = args.iter()
                    .map(|a| self.sql_to_rex(a, schema))
                    .collect::<Result<Vec<Expr>, String>>()?;

                //TODO: fix this hack
                if id.to_lowercase() == "min" {
                    Ok(Expr::AggregateFunction {
                        name: id.clone(),
                        args: rex_args,
                    })

                } else {
                    Ok(Expr::ScalarFunction {
                        name: id.clone(),
                        args: rex_args,
                    })
                }
            }

            _ => Err(String::from(format!(
                "Unsupported ast node {:?} in sqltorel",
                sql
            ))),
        }
    }
}

/// Convert SQL data type to relational representation of data type
pub fn convert_data_type(sql: &SQLType) -> DataType {
    match sql {
        &SQLType::Varchar(_) => DataType::Utf8,
        &SQLType::Int => DataType::Int32,
        &SQLType::Long => DataType::Int64,
        &SQLType::Float => DataType::Float64,
        &SQLType::Double => DataType::Float64,
    }
}


//pub fn create_projection(expr: Vec<Expr>, input: &LogicalPlan) -> LogicalPlan {
//    LogicalPlan::Projection {
//        expr: expr,
//        input: input.clone(),
//        schema: projection_schema.clone(),
//    }
//}

pub fn expr_to_field(expr: &Vec<Expr>, input_schema: &Schema) -> Vec<Field> {
    expr.iter()
        .map(|e| match e {
            &Expr::Column(i) => input_schema.columns[i].clone(),
            &Expr::ScalarFunction { ref name, .. } => Field {
                name: name.clone(),
                data_type: DataType::Float64, //TODO: hard-coded until I have function metadata in place
                nullable: true,
            },
            &Expr::AggregateFunction { ref name, .. } => Field {
                name: name.clone(),
                data_type: DataType::Float64, //TODO: hard-coded until I have function metadata in place
                nullable: true,
            },
            _ => unimplemented!(),
        })
        .collect()
}