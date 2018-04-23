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
use std::collections::HashSet;
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

                // selection first
                let selection_plan = match selection {
                    &Some(ref filter_expr) => Some(LogicalPlan::Selection {
                        expr: self.sql_to_rex(&filter_expr, &input_schema.clone())?,
                        input: input.clone(),
                        schema: input_schema.clone(),
                    }),
                    _ => None,
                };

                let expr: Vec<Expr> = projection
                    .iter()
                    .map(|e| self.sql_to_rex(&e, &input_schema))
                    .collect::<Result<Vec<Expr>, String>>()?;

                // collect aggregate expressions
                let aggr_expr: Vec<Expr> = expr.iter()
                    .filter(|e| match e {
                        Expr::AggregateFunction { .. } => true,
                        _ => false,
                    })
                    .map(|e| e.clone())
                    .collect();

                if aggr_expr.len() > 0 {


                    let aggregate_input: Rc<LogicalPlan> = match selection_plan {
                        Some(s) => Rc::new(s),
                        _ => input.clone(),
                    };

                    let group_expr: Vec<Expr> = match group_by {
                        Some(gbe) => gbe
                        .iter()
                        .map(|e| self.sql_to_rex(&e, &input_schema))
                        .collect::<Result<Vec<Expr>, String>>()?,
                        None => vec![]
                    };
                    //println!("GROUP BY: {:?}", group_expr);

                    let mut all_fields: Vec<Expr> = group_expr.clone();
                    aggr_expr.iter().for_each(|x| all_fields.push(x.clone()));

                    let aggr_schema = Schema::new(expr_to_field(&all_fields, input_schema));



                    //TODO: selection, projection, everything else
                    Ok(Rc::new(LogicalPlan::Aggregate {
                        input: aggregate_input,
                        group_expr,
                        aggr_expr,
                        schema: Rc::new(aggr_schema),
                    }))
                } else {
                    let projection_input: Rc<LogicalPlan> = match selection_plan {
                        Some(s) => Rc::new(s),
                        _ => input.clone(),
                    };

                    let projection_schema =
                        Rc::new(Schema::new(expr_to_field(&expr, input_schema.as_ref())));

                    let projection = LogicalPlan::Projection {
                        expr: expr,
                        input: projection_input,
                        schema: projection_schema.clone(),
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
                            let input_schema = projection.schema();
                            let order_by_rex: Result<Vec<Expr>, String> = order_by_expr
                                .iter()
                                .map(|e| self.sql_to_rex(e, &input_schema))
                                .collect();

                            LogicalPlan::Sort {
                                expr: order_by_rex?,
                                input: Rc::new(projection.clone()),
                                schema: input_schema.clone(),
                            }
                        }
                        _ => projection,
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
                    projection: None
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
            &ASTNode::SQLLiteralString(ref s) => Ok(Expr::Literal(ScalarValue::Utf8(Rc::new(s.clone())))),

            &ASTNode::SQLIdentifier(ref id) => {
                match schema.columns().iter().position(|c| c.name().eq(id)) {
                    Some(index) => Ok(Expr::Column(index)),
                    None => Err(format!(
                        "Invalid identifier '{}' for schema {}",
                        id,
                        schema.to_string()
                    )),
                }
            }

            &ASTNode::SQLCast { ref expr, ref data_type } => {
                Ok(Expr::Cast {
                    expr: Rc::new(self.sql_to_rex(&expr, schema)?),
                    data_type: convert_data_type(data_type)
                })
            }

            &ASTNode::SQLBinaryExpr {
                ref left,
                ref op,
                ref right,
            } => {
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
                    &SQLOperator::And => Operator::And,
                    &SQLOperator::Or => Operator::Or,
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
                match id.to_lowercase().as_ref() {
                    "min" | "max" | "count" | "sum" | "avg" => Ok(Expr::AggregateFunction {
                        name: id.clone(),
                        args: rex_args,
                    }),
                    _ => Ok(Expr::ScalarFunction {
                        name: id.clone(),
                        args: rex_args,
                    }),
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
            &Expr::Column(i) => input_schema.columns()[i].clone(),
            &Expr::ScalarFunction { ref name, .. } => Field::new(
                name,
                DataType::Float64, //TODO: hard-coded until I have function metadata in place
                true,
            ),
            &Expr::AggregateFunction { ref name, .. } => Field::new(
                name,
                DataType::Float64, //TODO: hard-coded until I have function metadata in place
                true,
            ),
            _ => unimplemented!(),
        })
        .collect()
}


fn collect_expr(e: &Expr, accum: &mut HashSet<usize>) {
    match e {
        Expr::Column(i) => {
            accum.insert(*i);
        },
        Expr::Cast { ref expr, .. } => {
            collect_expr(expr, accum)
        },
        Expr::Literal(_) => {}
        Expr::BinaryExpr { ref left, ref right, .. } => {
            collect_expr(left, accum);
            collect_expr(right, accum);
        }
        Expr::AggregateFunction { ref args, .. } => {
            args.iter().for_each(|e| collect_expr(e, accum));
        }
        Expr::ScalarFunction { ref args, .. } => {
            args.iter().for_each(|e| collect_expr(e, accum));
        }
        Expr::Sort { ref expr, .. } => {
            collect_expr(expr, accum)
        }
    }
}


pub fn push_down_projection(plan: &Rc<LogicalPlan>, projection: HashSet<usize>) -> Rc<LogicalPlan> {
    println!("push_down_projection() projection={:?}", projection);
    match plan.as_ref() {
        LogicalPlan::Aggregate { ref input, ref group_expr, ref aggr_expr, ref schema } => {
            //TODO: apply projection first
            let mut accum: HashSet<usize> = HashSet::new();
            group_expr.iter().for_each(|e| collect_expr(e, &mut accum));
            aggr_expr.iter().for_each(|e| collect_expr(e, &mut accum));
            Rc::new(LogicalPlan::Aggregate {
                input: push_down_projection(&input, accum),
                group_expr: group_expr.clone(),
                aggr_expr: aggr_expr.clone(),
                schema: schema.clone()
            })
        }
        LogicalPlan::Selection { ref expr, ref input, ref schema } => {
            let mut accum: HashSet<usize> = projection.clone();
            collect_expr(expr, &mut accum);
            Rc::new(LogicalPlan::Selection {
                expr: expr.clone(),
                input: push_down_projection(&input, accum),
                schema: schema.clone()
            })
        }
        LogicalPlan::TableScan { ref schema_name, ref table_name, ref schema, .. } => {
            Rc::new(LogicalPlan::TableScan {
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
                schema: schema.clone(),
                projection: Some(projection.iter().map(|i|*i).collect())
            })
        },
        LogicalPlan::CsvFile { ref filename, ref schema, ref has_header, .. } => {
            Rc::new(LogicalPlan::CsvFile {
                filename: filename.to_string(),
                schema: schema.clone(),
                has_header: *has_header,
                projection: Some(projection.iter().map(|i|*i).collect())
            })
        },
        _ => unimplemented!()
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use super::super::sqlparser::*;

    #[test]
    fn test_collect_expr() {

        // define schema for data source (csv file)
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("employee_name", DataType::Utf8, false),
            Field::new("job_title", DataType::Utf8, false),
            Field::new("base_pay", DataType::Utf8, false),
            Field::new("overtime_pay", DataType::Utf8, false),
            Field::new("other_pay", DataType::Utf8, false),
            Field::new("benefits", DataType::Utf8, false),
            Field::new("total_pay", DataType::Utf8, false),
            Field::new("total_pay_benefits", DataType::Utf8, false),
            Field::new("year", DataType::Utf8, false),
            Field::new("notes", DataType::Utf8, true),
            Field::new("agency", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]);
        let mut accum: HashSet<usize> = HashSet::new();
        collect_expr(&Expr::Cast { expr: Rc::new(Expr::Column(3)), data_type: DataType::Float64 }, &mut accum);
        collect_expr(&Expr::Cast { expr: Rc::new(Expr::Column(3)), data_type: DataType::Float64 }, &mut accum);
        println!("accum: {:?}", accum);
        assert_eq!(1, accum.len());
        assert!(accum.contains(&3));
    }

    #[test]
    fn test_push_down_projection_aggregate_query() {

        // define schema for data source (csv file)
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("employee_name", DataType::Utf8, false),
            Field::new("job_title", DataType::Utf8, false),
            Field::new("base_pay", DataType::Utf8, false),
            Field::new("overtime_pay", DataType::Utf8, false),
            Field::new("other_pay", DataType::Utf8, false),
            Field::new("benefits", DataType::Utf8, false),
            Field::new("total_pay", DataType::Utf8, false),
            Field::new("total_pay_benefits", DataType::Utf8, false),
            Field::new("year", DataType::Utf8, false),
            Field::new("notes", DataType::Utf8, true),
            Field::new("agency", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]);

        let schemas: Rc<RefCell<HashMap<String, Rc<Schema>>>> = Rc::new(RefCell::new(HashMap::new()));
        schemas.borrow_mut().insert("salaries".to_string(), Rc::new(schema));

        // define the SQL statement
        let sql = "SELECT year, MIN(CAST(base_pay AS FLOAT)), MAX(CAST(base_pay AS FLOAT)) \
                            FROM salaries \
                            WHERE base_pay != 'Not Provided' AND base_pay != '' \
                            GROUP BY year";

        let ast = Parser::parse_sql(String::from(sql)).unwrap();
        let query_planner = SqlToRel::new(schemas.clone());
        let plan = query_planner.sql_to_rel(&ast).unwrap();
        println!("BEFORE: {:?}", plan);

        let new_plan = push_down_projection(&plan, HashSet::new());
        println!("AFTER: {:?}", new_plan);

        //TODO: assertions

    }




}
