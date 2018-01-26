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

#[derive(Debug,Clone,PartialEq)]
pub enum ASTNode {
    SQLIdentifier{id: String, parts: Vec<String>},
    SQLBinaryExpr{left: Box<ASTNode>, op: SQLOperator, right: Box<ASTNode>},
    SQLNested(Box<ASTNode>),
    SQLUnary{operator: SQLOperator, rex: Box<ASTNode>},
    SQLLiteralInt(i64),
    SQLSelect{
        projection: Vec<ASTNode>,
        relation: Option<Box<ASTNode>>,
        selection: Option<Box<ASTNode>>,
        order: Option<Box<ASTNode>>,
        limit: Option<Box<ASTNode>>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum SQLOperator {
    ADD,
    SUB,
    MULT,
    DIV,
    MOD,
    GT,
    LT,
    GTEQ,
    LTEQ,
    EQ,
    NEQ,
    OR,
    AND
}
