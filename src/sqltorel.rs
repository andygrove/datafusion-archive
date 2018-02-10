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
                ..
            } => {
                // parse the input relation so we have access to the tuple type
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
                        &Expr::TupleValue(i) => input_schema.columns[i].clone(),
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

                let limit_plan = match limit {
                    &Some(ref limit_ast_node) => {
                        let limit_count = match **limit_ast_node {
                            ASTNode::SQLLiteralInt(n) => n,
                            _ => return Err(String::from("LIMIT parameter is not a number")),
                        };
                        LogicalPlan::Limit {
                            limit: limit_count as usize,
                            schema: selection_plan.schema(),
                            input: Box::new(selection_plan),
                        }
                    }
                    _ => selection_plan,
                };

                Ok(Box::new(limit_plan))
            }

            &ASTNode::SQLIdentifier { ref id, .. } => {
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

    pub fn sql_to_rex(&self, sql: &ASTNode, tt: &Schema) -> Result<Expr, String> {
        match sql {

            &ASTNode::SQLLiteralInt(n) =>
                Ok(Expr::Literal(Value::UnsignedLong(n as u64))), //TODO

            &ASTNode::SQLIdentifier { ref id, .. } => {
                match tt.columns.iter().position(|c| c.name.eq(id) ) {
                    Some(index) => Ok(Expr::TupleValue(index)),
                    None => Err(format!("Invalid identifier {}", id))
                }
            },

            &ASTNode::SQLBinaryExpr { ref left, ref op, ref right } => {
                //TODO: we have this implemented somewhere else already
                let operator = match op {
                    &SQLOperator::GT => Operator::Gt,
                    &SQLOperator::GTEQ => Operator::GtEq,
                    &SQLOperator::LT => Operator::Lt,
                    &SQLOperator::LTEQ => Operator::LtEq,
                    &SQLOperator::EQ => Operator::Eq,
                    _ => unimplemented!()
                };
                Ok(Expr::BinaryExpr {
                    left: Box::new(self.sql_to_rex(&left, &tt)?),
                    op: operator,
                    right: Box::new(self.sql_to_rex(&right, &tt)?),
                })

            },

            &ASTNode::SQLFunction { ref id, ref args } => {
                let rex_args = args.iter()
                    .map(|a| self.sql_to_rex(a, tt))
                    .collect::<Result<Vec<Expr>, String>>()?;

                Ok(Expr::ScalarFunction { name: id.clone(), args: rex_args })
            },

            _ => Err(String::from(format!("Unsupported ast node {:?}", sql)))
        }
    }

}

/// Convert SQL data type to relational representation of data type
pub fn convert_data_type(sql: &SQLType) -> DataType {
    match sql {
        &SQLType::Varchar(_) => DataType::String,
        &SQLType::Double => DataType::Double
    }
}

