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

//! SQL Parser
//!
//! Note that most SQL parsing is now delegated to the sqlparser crate, which handles ANSI SQL but
//! this module contains DataFusion-specific SQL extensions.

use std::fmt;
use std::fmt::{Error, Formatter};
use std::rc::Rc;

use super::types::*;

use arrow::datatypes::*;
use sqlparser::sqlast::*;
use sqlparser::sqlparser::*;
use sqlparser::sqltokenizer::*;

#[derive(Debug, Clone)]
pub enum FileType {
    NdJson,
    Parquet,
    CSV
}

#[derive(Debug, Clone)]
pub enum DFASTNode {
    /// ANSI SQL AST node
    ANSI(ASTNode),
    /// DDL for creating an external table in DataFusion
    CreateExternalTable {
        /// Table name
        name: String,
        /// Optional schema
        columns: Vec<SQLColumnDef>,
        /// File type (Parquet, NDJSON, CSV)
        file_type: FileType,
        /// Header row?
        header_row: bool,
        /// Path to file
        location: String
    },
}


/// SQL Parser
pub struct DFParser {
    parser: Parser
}

impl DFParser {

    /// Parse the specified tokens
    pub fn new(sql: String) -> Result<Self, ParserError> {
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize()?;
        Ok(DFParser {
            parser: Parser::new(tokens)
        })
    }

    /// Parse a SQL statement and produce an Abstract Syntax Tree (AST)
    pub fn parse_sql(sql: String) -> Result<DFASTNode, ParserError> {
        let mut parser = DFParser::new(sql)?;
        parser.parse()
    }

    /// Parse a new expression
    pub fn parse(&mut self) -> Result<DFASTNode, ParserError> {
        self.parse_expr(0)
    }

    /// Parse tokens until the precedence changes
    fn parse_expr(&mut self, precedence: u8) -> Result<DFASTNode, ParserError> {
        let mut expr = self.parse_prefix()?;
        loop {
            let next_precedence = self.parser.get_next_precedence()?;
            if precedence >= next_precedence {
                break;
            }

            if let Some(infix_expr) = self.parse_infix(expr.clone(), next_precedence)? {
                expr = infix_expr;
            }
        }
        Ok(expr)
    }

    /// Parse an expression prefix
    fn parse_prefix(&mut self) -> Result<DFASTNode, ParserError> {
        if self.parser.parse_keywords(vec!["CREATE", "EXTERNAL", "TABLE"]) {
            unimplemented!()
        } else {
            Ok(DFASTNode::ANSI(self.parser.parse_prefix()?))
        }
    }

    pub fn parse_infix(
        &mut self,
        expr: DFASTNode,
        precedence: u8,
    ) -> Result<Option<DFASTNode>, ParserError> {

//        match expr {
//            DFASTNode::ANSI(ansi) => {
//                //DFASTNode::ANSI(self.parser.parse_infix(ansi, precedence)?)
//            })

        unimplemented!()
    }
}
