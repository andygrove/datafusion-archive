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
use std::string::String;

use super::sql::*;
use super::rel::*;

pub struct SqlToRel {
    //default_schema: Option<String>,
    schemas: HashMap<String, Schema>
}

impl SqlToRel {

    pub fn new(schemas: HashMap<String, Schema>) -> Self {
        SqlToRel { /*default_schema: None,*/ schemas }
    }

    pub fn sql_to_rel(&self, sql: &ASTNode) -> Result<Box<LogicalPlan>, String> {
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
                    &None => Box::new(LogicalPlan::EmptyRelation)
                };

                let input_schema = input.schema();

                let expr : Vec<Expr> = projection.iter()
                    .map(|e| self.sql_to_rex(&e, &input_schema) )
                    .collect::<Result<Vec<Expr>,String>>()?;


                let projection_schema = Schema {
                    columns: expr.iter().map( |e| match e {
                        &Expr::Column(i) => input_schema.columns[i].clone(),
                        &Expr::ScalarFunction { ref name, .. } => Field {
                            name: name.clone(),
                            data_type: DataType::Double, //TODO: hard-coded until I have function metadata in place
                            nullable: true
                        },
                        _ => unimplemented!()
                    }).collect()
                };

                let selection_plan = match selection {
                    &Some(ref filter_expr) => {

                        let selection_rel = LogicalPlan::Selection {
                            expr: self.sql_to_rex(&filter_expr, &input_schema.clone())?,
                            input: input,
                            schema: input_schema.clone()
                        };

                        LogicalPlan::Projection {
                            expr: expr,
                            input: Box::new(selection_rel),
                            schema: projection_schema.clone(),
                        }
                    }
                    _ => LogicalPlan::Projection {
                        expr: expr,
                        input: input,
                        schema: projection_schema.clone(),
                    },
                };

                if let &Some(_) = group_by {
                    return Err(String::from("GROUP BY is not implemented yet"))
                }

                if let &Some(_) = having {
                    return Err(String::from("HAVING is not implemented yet"))
                }

                let order_by_plan = match order_by {
                    &Some(ref order_by_expr) => {
                        let input_schema = selection_plan.schema();
                        let order_by_rex : Result<Vec<Expr>, String> = order_by_expr.iter()
                            .map(|e| self.sql_to_rex(e, &input_schema))
                            .collect();

                        LogicalPlan::Sort {
                            expr: order_by_rex?,
                            input: Box::new(selection_plan),
                            schema: input_schema,
                        }
                    },
                    _ => selection_plan
                };

                let limit_plan = match limit {
                    &Some(ref limit_ast_node) => {
                        let limit_count = match **limit_ast_node {
                            ASTNode::SQLLiteralInt(n) => n,
                            _ => return Err(String::from("LIMIT parameter is not a number")),
                        };
                        LogicalPlan::Limit {
                            limit: limit_count as usize,
                            schema: order_by_plan.schema(),
                            input: Box::new(order_by_plan),
                        }
                    }
                    _ => order_by_plan,
                };

                Ok(Box::new(limit_plan))
            }

            &ASTNode::SQLIdentifier(ref id) => {
                match self.schemas.get(id) {
                    Some(schema) => Ok(Box::new(LogicalPlan::TableScan {
                        schema_name: String::from("default"),
                        table_name: id.clone(),
                        schema: schema.clone()
                    })),
                    None => Err(format!("no schema found for table {}", id))
                }
            },

            _ => Err(format!("sql_to_rel does not support this relation: {:?}", sql))
        }
    }

    pub fn sql_to_rex(&self, sql: &ASTNode, schema: &Schema) -> Result<Expr, String> {
        match sql {

            &ASTNode::SQLLiteralInt(n) =>
                Ok(Expr::Literal(Value::Long(n as i64))),

            &ASTNode::SQLIdentifier(ref id) => {
                match schema.columns.iter().position(|c| c.name.eq(id) ) {
                    Some(index) => Ok(Expr::Column(index)),
                    None => Err(format!("Invalid identifier {}", id))
                }
            },

            &ASTNode::SQLBinaryExpr { ref left, ref op, ref right } => {
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
                    &SQLOperator::Modulus => Operator::Modulus
                };
                Ok(Expr::BinaryExpr {
                    left: Box::new(self.sql_to_rex(&left, &schema)?),
                    op: operator,
                    right: Box::new(self.sql_to_rex(&right, &schema)?),
                })

            },

            &ASTNode::SQLOrderBy { ref expr, asc } =>
                Ok(Expr::Sort { expr: Box::new(self.sql_to_rex(&expr, &schema)?), asc }),

            &ASTNode::SQLFunction { ref id, ref args } => {
                let rex_args = args.iter()
                    .map(|a| self.sql_to_rex(a, schema))
                    .collect::<Result<Vec<Expr>, String>>()?;

                Ok(Expr::ScalarFunction { name: id.clone(), args: rex_args })
            },

            _ => Err(String::from(format!("Unsupported ast node {:?} in sqltorel", sql)))
        }
    }

}

/// Convert SQL data type to relational representation of data type
pub fn convert_data_type(sql: &SQLType) -> DataType {
    match sql {
        &SQLType::Varchar(_) => DataType::String,
        &SQLType::Int => DataType::UnsignedLong,
        &SQLType::Long => DataType::UnsignedLong,
        &SQLType::Float => DataType::Double,
        &SQLType::Double => DataType::Double,
    }
}

