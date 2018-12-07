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

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use super::super::logicalplan::Expr;
use super::context::ExecutionContext;
use super::error::{ExecutionError, Result};

/// Compiled Expression (basically just a closure to evaluate the expression at runtime)
pub type CompiledExpr = Rc<Fn(&RecordBatch) -> Result<ArrayRef>>;

pub type CompiledCastFunction = Rc<Fn(&ArrayRef) -> Result<ArrayRef>>;

pub enum AggregateType {
    Min,
    Max,
    Sum,
    Count,
    Avg,
    //CountDistinct()
}

/// Runtime expression
pub enum RuntimeExpr {
    Compiled {
        f: CompiledExpr,
        t: DataType,
    },
    AggregateFunction {
        f: AggregateType,
        args: Vec<CompiledExpr>,
        t: DataType,
    },
}

impl RuntimeExpr {
    pub fn get_func(&self) -> CompiledExpr {
        match self {
            &RuntimeExpr::Compiled { ref f, .. } => f.clone(),
            _ => panic!(),
        }
    }
    pub fn get_type(&self) -> DataType {
        match self {
            &RuntimeExpr::Compiled { ref t, .. } => t.clone(),
            &RuntimeExpr::AggregateFunction { ref t, .. } => t.clone(),
        }
    }
}

/// Compiles a scalar expression into a closure
pub fn compile_expr(
    ctx: Rc<ExecutionContext>,
    expr: &Expr,
    input_schema: &Schema,
) -> Result<RuntimeExpr> {
    match *expr {
        Expr::AggregateFunction {
            ref name,
            ref args,
            ref return_type,
        } => {
            assert_eq!(1, args.len());

            let compiled_args: Result<Vec<RuntimeExpr>> = args
                .iter()
                .map(|e| compile_scalar_expr(&ctx, e, input_schema))
                .collect();

            let func = match name.to_lowercase().as_ref() {
                "min" => AggregateType::Min,
                "max" => AggregateType::Max,
                "count" => AggregateType::Count,
                "sum" => AggregateType::Sum,
                _ => unimplemented!("Unsupported aggregate function '{}'", name),
            };

            Ok(RuntimeExpr::AggregateFunction {
                f: func,
                args: compiled_args?
                    .iter()
                    .map(|e| e.get_func().clone())
                    .collect(),
                t: return_type.clone(),
            })
        }
        _ => Ok(compile_scalar_expr(&ctx, expr, input_schema)?),
    }
}

/// Compiles a scalar expression into a closure
pub fn compile_scalar_expr(
    _ctx: &ExecutionContext,
    expr: &Expr,
    input_schema: &Schema,
) -> Result<RuntimeExpr> {
    match expr {
        &Expr::Literal(ref _lit) => {
            unimplemented!()
            //            let literal_value = lit.clone();
            //            Ok(RuntimeExpr::Compiled {
            //                f: Rc::new(move |_| {
            //                    // literal values are a bit special - we don't repeat them in a vector
            //                    // because it would be redundant, so we have a single value in a vector instead
            //                    Ok(Value::Scalar(Rc::new(literal_value.clone())))
            //                }),
            //                t: DataType::Float64, //TODO
            //            })
        }
        &Expr::Column(index) => Ok(RuntimeExpr::Compiled {
            f: Rc::new(move |batch: &RecordBatch| Ok((*batch.column(index)).clone())),
            t: input_schema.field(index).data_type().clone(),
        }),
        &Expr::Cast {
            ref expr,
            ..
        } => match expr.as_ref() {
            &Expr::Column(_index) => {
                unimplemented!()
                //                let compiled_cast_expr = compile_cast_column(data_type.clone())?;
                //                Ok(RuntimeExpr::Compiled {
                //                    f: Rc::new(move |batch: &RecordBatch| {
                //                        (compiled_cast_expr)(batch.column(index))
                //                    }),
                //                    t: data_type.clone(),
                //                })
            }
            &Expr::Literal(ref _lit) => {
                unimplemented!()
                //                let compiled_cast_expr = compile_cast_scalar(lit, data_type)?;
                //                Ok(RuntimeExpr::Compiled {
                //                    f: Rc::new(move |_: &RecordBatch| {
                //                        (compiled_cast_expr)(&Value::Scalar(Rc::new(ScalarValue::Null))) // pointless arg
                //                    }),
                //                    t: data_type.clone(),
                //                })
            }
            other => Err(ExecutionError::General(format!(
                "CAST not implemented for expression {:?}",
                other
            ))),
        },
        //        &Expr::IsNotNull(ref expr) => {
        //            let compiled_expr = compile_scalar_expr(ctx, expr, input_schema)?;
        //            Ok(RuntimeExpr::Compiled {
        //                f: Rc::new(move |batch: &RecordBatch| {
        //                    let left_values = compiled_expr.get_func()(batch)?;
        //                    left_values.is_not_null()
        //                }),
        //                t: DataType::Boolean,
        //            })
        //        }
        //        &Expr::IsNull(ref expr) => {
        //            let compiled_expr = compile_scalar_expr(ctx, expr, input_schema)?;
        //            Ok(RuntimeExpr::Compiled {
        //                f: Rc::new(move |batch: &RecordBatch| {
        //                    let left_values = compiled_expr.get_func()(batch)?;
        //                    left_values.is_null()
        //                }),
        //                t: DataType::Boolean,
        //            })
        //        }
        //        &Expr::BinaryExpr {
        //            ref left,
        //            ref op,
        //            ref right,
        //        } => {
        //            let left_expr = compile_scalar_expr(ctx, left, input_schema)?;
        //            let right_expr = compile_scalar_expr(ctx, right, input_schema)?;
        //            let op_type = left_expr.get_type().clone();
        //            match op {
        //                &Operator::Eq => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.eq(&right_values)
        //                    }),
        //                    t: DataType::Boolean,
        //                }),
        //                &Operator::NotEq => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.not_eq(&right_values)
        //                    }),
        //                    t: DataType::Boolean,
        //                }),
        //                &Operator::Lt => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.lt(&right_values)
        //                    }),
        //                    t: DataType::Boolean,
        //                }),
        //                &Operator::LtEq => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.lt_eq(&right_values)
        //                    }),
        //                    t: DataType::Boolean,
        //                }),
        //                &Operator::Gt => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.gt(&right_values)
        //                    }),
        //                    t: DataType::Boolean,
        //                }),
        //                &Operator::GtEq => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.gt_eq(&right_values)
        //                    }),
        //                    t: DataType::Boolean,
        //                }),
        //                &Operator::And => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.and(&right_values)
        //                    }),
        //                    t: DataType::Boolean,
        //                }),
        //                &Operator::Or => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.or(&right_values)
        //                    }),
        //                    t: DataType::Boolean,
        //                }),
        //                &Operator::Plus => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.add(&right_values)
        //                    }),
        //                    t: op_type,
        //                }),
        //                &Operator::Minus => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.subtract(&right_values)
        //                    }),
        //                    t: op_type,
        //                }),
        //                &Operator::Multiply => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.multiply(&right_values)
        //                    }),
        //                    t: op_type,
        //                }),
        //                &Operator::Divide => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.divide(&right_values)
        //                    }),
        //                    t: op_type,
        //                }),
        //                &Operator::Modulus => Ok(RuntimeExpr::Compiled {
        //                    f: Rc::new(move |batch: &RecordBatch| {
        //                        let left_values = left_expr.get_func()(batch)?;
        //                        let right_values = right_expr.get_func()(batch)?;
        //                        left_values.modulo(&right_values)
        //                    }),
        //                    t: op_type,
        //                }),
        //            }
        //        }
        //        &Expr::Sort { ref expr, .. } => {
        //            //NOTE sort order is ignored here and is handled during sort execution
        //            compile_scalar_expr(ctx, expr, input_schema)
        //        }
        //        &Expr::ScalarFunction {
        //            ref name,
        //            ref args,
        //            ref return_type,
        //        } => {
        //            ////println!("Executing function {}", name);
        //
        //            let func = ctx.load_scalar_function(name.as_ref())?;
        //
        //            let expected_args = func.args();
        //
        //            if expected_args.len() != args.len() {
        //                return Err(ExecutionError::General(format!(
        //                    "Function {} requires {} parameters but {} were provided",
        //                    name,
        //                    expected_args.len(),
        //                    args.len()
        //                )));
        //            }
        //
        //            // evaluate the arguments to the function
        //            let compiled_args: Result<Vec<RuntimeExpr>> = args
        //                .iter()
        //                .map(|e| compile_scalar_expr(ctx, e, input_schema))
        //                .collect();
        //
        //            let compiled_args_ok = compiled_args?;
        //
        //            // type checking for function arguments
        //            for i in 0..expected_args.len() {
        //                let actual_type = compiled_args_ok[i].get_type();
        //                if expected_args[i].data_type() != &actual_type {
        //                    return Err(ExecutionError::General(format!(
        //                        "Scalar function {} requires {:?} for argument {} but got {:?}",
        //                        name,
        //                        expected_args[i].data_type(),
        //                        i,
        //                        actual_type
        //                    )));
        //                }
        //            }
        //
        //            Ok(RuntimeExpr::Compiled {
        //                f: Rc::new(move |batch| {
        //                    let arg_values: Result<Vec<ArrayRef>> = compiled_args_ok
        //                        .iter()
        //                        .map(|expr| expr.get_func()(batch))
        //                        .collect();
        //
        //                    func.execute(&arg_values?)
        //                }),
        //                t: return_type.clone(),
        //            })
        //        }
        //        // aggregate functions don't fit this pattern .. will need to rework this ..
        //        &Expr::AggregateFunction { .. } => panic!("Aggregate expressions cannot be compiled yet"),
        //        //        &Expr::AggregateFunction { ref name, ref args } => {
        //        //
        //        //            // evaluate the arguments to the function
        //        //            let compiled_args: Result<Vec<CompiledExpr>> =
        //        //                args.iter().map(|e| compile_expr(ctx, e)).collect();
        //        //
        //        //            let compiled_args_ok = compiled_args?;
        //        //
        //        //            Ok(Rc::new(move |batch| {
        //        //                let arg_values: Result<Vec<Value>> =
        //        //                    compiled_args_ok.iter().map(|expr| expr(batch)).collect();
        //        //
        //        //                Ok(Rc::new(arg_values?))
        //        //            }))
        //        //        }
        _ => panic!(),
    }
}
