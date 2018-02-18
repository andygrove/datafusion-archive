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

use super::rel::*;

#[derive(Debug)]
pub enum CompilerError {
    TBD(String)
}

/// Compiled Expression (basically just a closure to evaluate the expression at runtime)
type CompiledExpr = Box<Fn(&Row)-> Value>;

/// Compiles a relational expression into a closure
pub fn compile_expr(expr: Expr) -> Result<CompiledExpr, Box<CompilerError>> {
    match expr {
        Expr::Literal(lit) => {
            Ok(Box::new(move |_| lit.clone()))
        },
        Expr::BinaryExpr { left, op, right } => {
            let l = compile_expr(*left)?;
            let r = compile_expr(*right)?;
            match op {
                Operator::Eq => Ok(Box::new(move |row| Value::Boolean(l(row) == r(row)))),
                _ => unimplemented!()
            }
        }
        _ => unimplemented!()
    }
}