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

use super::sqlast::*;
use super::sqltokenizer::*;

#[derive(Debug, Clone)]
pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
}

impl From<TokenizerError> for ParserError {
    fn from(e: TokenizerError) -> Self {
        ParserError::TokenizerError(format!("{:?}", e))
    }
}

/// SQL Parser
pub struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    /// Parse the specified tokens
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser {
            tokens: tokens,
            index: 0,
        }
    }

    /// Parse a SQL statement and produce an Abstract Syntax Tree (AST)
    pub fn parse_sql(sql: String) -> Result<ASTNode, ParserError> {
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens);
        parser.parse()
    }

    /// Parse a new expression
    pub fn parse(&mut self) -> Result<ASTNode, ParserError> {
        self.parse_expr(0)
    }

    /// Parse tokens until the precedence changes
    fn parse_expr(&mut self, precedence: u8) -> Result<ASTNode, ParserError> {
        //        println!("parse_expr() precendence = {}", precedence);

        let mut expr = self.parse_prefix()?;
        //        println!("parsed prefix: {:?}", expr);

        loop {
            let next_precedence = self.get_next_precedence()?;
            if precedence >= next_precedence {
                //                println!("break on precedence change ({} >= {})", precedence, next_precedence);
                break;
            }

            if let Some(infix_expr) = self.parse_infix(expr.clone(), next_precedence)? {
                //                println!("parsed infix: {:?}", infix_expr);
                expr = infix_expr;
            }
        }

        //        println!("parse_expr() returning {:?}", expr);

        Ok(expr)
    }

    /// Parse an expression prefix
    fn parse_prefix(&mut self) -> Result<ASTNode, ParserError> {
        match self.next_token() {
            Some(t) => {
                match t {
                    Token::Keyword(k) => match k.to_uppercase().as_ref() {
                        "SELECT" => Ok(self.parse_select()?),
                        "CREATE" => Ok(self.parse_create()?),
                        _ => Err(ParserError::ParserError(format!(
                            "No prefix parser for keyword {}",
                            k
                        ))),
                    },
                    Token::Identifier(id) => {
                        match self.peek_token() {
                            Some(Token::LParen) => {
                                self.next_token(); // skip lparen

                                let args = self.parse_expr_list()?;

                                self.next_token(); // skip rparen

                                Ok(ASTNode::SQLFunction { id, args })
                            }
                            Some(Token::Period) => {
                                let mut id_parts: Vec<String> = vec![id];
                                while self.peek_token() == Some(Token::Period) {
                                    self.consume_token(&Token::Period)?;
                                    match self.next_token() {
                                        Some(Token::Identifier(id)) => id_parts.push(id),
                                        _ => {
                                            return Err(ParserError::ParserError(format!(
                                                "Error parsing compound identifier"
                                            )))
                                        }
                                    }
                                }
                                Ok(ASTNode::SQLCompoundIdentifier(id_parts))
                            }
                            _ => Ok(ASTNode::SQLIdentifier(id)),
                        }
                    }
                    Token::Number(ref n) if n.contains(".") => match n.parse::<f64>() {
                        Ok(n) => Ok(ASTNode::SQLLiteralDouble(n)),
                        Err(e) => Err(ParserError::ParserError(format!(
                            "Could not parse '{}' as i64: {}",
                            n, e
                        ))),
                    },
                    Token::Number(ref n) => match n.parse::<i64>() {
                        Ok(n) => Ok(ASTNode::SQLLiteralLong(n)),
                        Err(e) => Err(ParserError::ParserError(format!(
                            "Could not parse '{}' as i64: {}",
                            n, e
                        ))),
                    },
                    _ => Err(ParserError::ParserError(format!(
                        "Prefix parser expected a keyword but found {:?}",
                        t
                    ))),
                }
            }
            None => Err(ParserError::ParserError(format!(
                "Prefix parser expected a keyword but hit EOF"
            ))),
        }
    }

    /// Parse an expression infix
    fn parse_infix(
        &mut self,
        expr: ASTNode,
        precedence: u8,
    ) -> Result<Option<ASTNode>, ParserError> {
        match self.next_token() {
            Some(tok) => match tok {
                Token::Keyword(_) => Ok(Some(ASTNode::SQLBinaryExpr {
                    left: Box::new(expr),
                    op: self.to_sql_operator(&tok)?,
                    right: Box::new(self.parse_expr(precedence)?),
                })),
                Token::Eq
                | Token::Gt
                | Token::GtEq
                | Token::Lt
                | Token::LtEq
                | Token::Plus
                | Token::Minus
                | Token::Mult
                | Token::Div => Ok(Some(ASTNode::SQLBinaryExpr {
                    left: Box::new(expr),
                    op: self.to_sql_operator(&tok)?,
                    right: Box::new(self.parse_expr(precedence)?),
                })),
                _ => Err(ParserError::ParserError(format!(
                    "No infix parser for token {:?}",
                    tok
                ))),
            },
            None => Ok(None),
        }
    }

    /// Convert a token operator to an AST operator
    fn to_sql_operator(&self, tok: &Token) -> Result<SQLOperator, ParserError> {
        match tok {
            &Token::Eq => Ok(SQLOperator::Eq),
            &Token::Lt => Ok(SQLOperator::Lt),
            &Token::LtEq => Ok(SQLOperator::LtEq),
            &Token::Gt => Ok(SQLOperator::Gt),
            &Token::GtEq => Ok(SQLOperator::GtEq),
            &Token::Plus => Ok(SQLOperator::Plus),
            &Token::Minus => Ok(SQLOperator::Minus),
            &Token::Mult => Ok(SQLOperator::Multiply),
            &Token::Div => Ok(SQLOperator::Divide),
            _ => Err(ParserError::ParserError(format!(
                "Unsupported SQL operator {:?}",
                tok
            ))),
        }
    }

    /// Get the precedence of the next token
    fn get_next_precedence(&self) -> Result<u8, ParserError> {
        if self.index < self.tokens.len() {
            self.get_precedence(&self.tokens[self.index])
        } else {
            Ok(0)
        }
    }

    /// Get the precedence of a token
    fn get_precedence(&self, tok: &Token) -> Result<u8, ParserError> {
        //println!("get_precedence() {:?}", tok);

        match tok {
            &Token::Keyword(ref k) if k == "OR" => Ok(5),
            &Token::Keyword(ref k) if k == "AND" => Ok(10),
            &Token::Eq | &Token::Lt | &Token::LtEq | &Token::Neq | &Token::Gt | &Token::GtEq => {
                Ok(20)
            }
            &Token::Plus | &Token::Minus => Ok(30),
            &Token::Mult | &Token::Div => Ok(40),
            _ => Ok(0),
        }
    }

    /// Peek at the next token
    fn peek_token(&mut self) -> Option<Token> {
        if self.index < self.tokens.len() {
            Some(self.tokens[self.index].clone())
        } else {
            None
        }
    }

    /// Get the next token and increment the token index
    fn next_token(&mut self) -> Option<Token> {
        if self.index < self.tokens.len() {
            self.index = self.index + 1;
            Some(self.tokens[self.index - 1].clone())
        } else {
            None
        }
    }

    /// Get the previous token and decrement the token index
    fn prev_token(&mut self) -> Option<Token> {
        if self.index > 0 {
            Some(self.tokens[self.index - 1].clone())
        } else {
            None
        }
    }

    /// Look for an expected keyword and consume it if it exists
    fn parse_keyword(&mut self, expected: &'static str) -> bool {
        match self.peek_token() {
            Some(Token::Keyword(k)) => {
                if expected.eq_ignore_ascii_case(k.as_str()) {
                    self.next_token();
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Look for an expected sequence of keywords and consume them if they exist
    fn parse_keywords(&mut self, keywords: Vec<&'static str>) -> bool {
        let index = self.index;
        for keyword in keywords {
            if !self.parse_keyword(&keyword) {
                self.index = index;
                return false;
            }
        }
        true
    }

    //    fn parse_identifier(&mut self) -> Result<ASTNode::SQLIdentifier, Err> {
    //        let expr = self.parse_expr()?;
    //        match expr {
    //            Some(ASTNode::SQLIdentifier { .. }) => Ok(expr),
    //            _ => Err(ParserError::ParserError(format!("Expected identifier but found {:?}", expr)))
    //        }
    //    }

    /// Consume the next token if it matches the expected token, otherwise return an error
    fn consume_token(&mut self, expected: &Token) -> Result<(), ParserError> {
        match self.next_token() {
            Some(ref t) if *t == *expected => Ok(()),
            _ => Err(ParserError::ParserError(format!(
                "expected token {:?} but was {:?}",
                expected,
                self.prev_token()
            ))),
        }
    }

    // specific methods

    /// Parse a SQL CREATE statement
    fn parse_create(&mut self) -> Result<ASTNode, ParserError> {
        if self.parse_keywords(vec!["EXTERNAL", "TABLE"]) {
            match self.next_token() {
                Some(Token::Identifier(id)) => {
                    self.consume_token(&Token::LParen)?;

                    let mut columns = vec![];

                    // parse column defs
                    loop {
                        if let Some(Token::Identifier(column_name)) = self.next_token() {
                            if let Ok(data_type) = self.parse_data_type() {
                                if self.parse_keywords(vec!["NOT", "NULL"]) {
                                    //TODO:
                                } else if self.parse_keyword("NULL") {
                                    //TODO:
                                }

                                match self.peek_token() {
                                    Some(Token::Comma) => {
                                        self.next_token();
                                        columns.push(SQLColumnDef {
                                            name: column_name,
                                            data_type: data_type,
                                            allow_null: true, // TODO
                                        });
                                    }
                                    Some(Token::RParen) => {
                                        self.next_token();
                                        columns.push(SQLColumnDef {
                                            name: column_name,
                                            data_type: data_type,
                                            allow_null: true, // TODO
                                        });
                                        break;
                                    }
                                    _ => {
                                        return Err(ParserError::ParserError(
                                            "Expected ',' or ')' after column definition"
                                                .to_string(),
                                        ))
                                    }
                                }
                            } else {
                                return Err(ParserError::ParserError(
                                    "Error parsing data type in column definition".to_string(),
                                ));
                            }
                        } else {
                            return Err(ParserError::ParserError(
                                "Error parsing column name".to_string(),
                            ));
                        }
                    }

                    Ok(ASTNode::SQLCreateTable {
                        name: id,
                        columns: columns,
                    })
                }
                _ => Err(ParserError::ParserError(format!(
                    "Unexpected token after CREATE EXTERNAL TABLE: {:?}",
                    self.peek_token()
                ))),
            }
        } else {
            Err(ParserError::ParserError(format!(
                "Unexpected token after CREATE: {:?}",
                self.peek_token()
            )))
        }
    }

    /// Parse a literal integer/long
    fn parse_literal_int(&mut self) -> Result<i64, ParserError> {
        match self.next_token() {
            Some(Token::Number(s)) => s.parse::<i64>().map_err(|e| {
                ParserError::ParserError(format!("Could not parse '{}' as i64: {}", s, e))
            }),
            _ => Err(ParserError::ParserError("Expected literal int".to_string())),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example)
    fn parse_data_type(&mut self) -> Result<SQLType, ParserError> {
        match self.next_token() {
            Some(Token::Keyword(k)) => match k.to_uppercase().as_ref() {
                "INT" | "INTEGER" => Ok(SQLType::Int),
                "LONG" => Ok(SQLType::Long),
                "FLOAT" => Ok(SQLType::Float),
                "DOUBLE" => Ok(SQLType::Double),
                "VARCHAR" => {
                    self.consume_token(&Token::LParen)?;
                    let n = self.parse_literal_int()?;
                    self.consume_token(&Token::RParen)?;
                    Ok(SQLType::Varchar(n as usize))
                }
                _ => Err(ParserError::ParserError("Invalid data type".to_string())),
            },
            _ => Err(ParserError::ParserError("Invalid data type".to_string())),
        }
    }

    /// Parse a SELECT statement
    fn parse_select(&mut self) -> Result<ASTNode, ParserError> {
        let projection = self.parse_expr_list()?;

        let relation: Option<Box<ASTNode>> = if self.parse_keyword("FROM") {
            //TODO: add support for JOIN
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        let selection = if self.parse_keyword("WHERE") {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        let group_by = if self.parse_keywords(vec!["GROUP", "BY"]) {
            Some(self.parse_expr_list()?)
        } else {
            None
        };

        let having = if self.parse_keyword("HAVING") {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        let order_by = if self.parse_keywords(vec!["ORDER", "BY"]) {
            Some(self.parse_order_by_expr_list()?)
        } else {
            None
        };

        let limit = if self.parse_keyword("LIMIT") {
            self.parse_limit()?
        } else {
            None
        };

        if let Some(next_token) = self.peek_token() {
            Err(ParserError::ParserError(format!(
                "Unexpected token at end of SELECT: {:?}",
                next_token
            )))
        } else {
            Ok(ASTNode::SQLSelect {
                projection,
                selection,
                relation,
                limit,
                order_by,
                group_by,
                having,
            })
        }
    }

    /// Parse a comma-delimited list of SQL expressions
    fn parse_expr_list(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        let mut expr_list: Vec<ASTNode> = vec![];
        loop {
            expr_list.push(self.parse_expr(0)?);
            if let Some(t) = self.peek_token() {
                if t == Token::Comma {
                    self.next_token();
                } else {
                    break;
                }
            } else {
                //EOF
                break;
            }
        }
        Ok(expr_list)
    }

    /// Parse a comma-delimited list of SQL ORDER BY expressions
    fn parse_order_by_expr_list(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        let mut expr_list: Vec<ASTNode> = vec![];
        loop {
            let expr = self.parse_expr(0)?;

            // look for optional ASC / DESC specifier
            let asc = match self.peek_token() {
                Some(Token::Keyword(k)) => {
                    self.next_token(); // consume it
                    match k.to_uppercase().as_ref() {
                        "ASC" => true,
                        "DESC" => false,
                        _ => {
                            return Err(ParserError::ParserError(format!(
                                "Invalid modifier for ORDER BY expression: {:?}",
                                k
                            )))
                        }
                    }
                }
                Some(Token::Comma) => true,
                Some(other) => {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token after ORDER BY expr: {:?}",
                        other
                    )))
                }
                None => true,
            };

            expr_list.push(ASTNode::SQLOrderBy {
                expr: Box::new(expr),
                asc,
            });

            if let Some(t) = self.peek_token() {
                if t == Token::Comma {
                    self.next_token();
                } else {
                    break;
                }
            } else {
                // EOF
                break;
            }
        }
        Ok(expr_list)
    }

    /// Parse a LIMIT clause
    fn parse_limit(&mut self) -> Result<Option<Box<ASTNode>>, ParserError> {
        if self.parse_keyword("ALL") {
            Ok(None)
        } else {
            self.parse_literal_int()
                .map(|n| Some(Box::new(ASTNode::SQLLiteralLong(n))))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn tokenize_select_1() {
        let sql = String::from("SELECT 1");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword(String::from("SELECT")),
            Token::Number(String::from("1")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_scalar_function() {
        let sql = String::from("SELECT sqrt(1)");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword(String::from("SELECT")),
            Token::Identifier(String::from("sqrt")),
            Token::LParen,
            Token::Number(String::from("1")),
            Token::RParen,
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_simple_select() {
        let sql = String::from("SELECT * FROM customer WHERE id = 1 LIMIT 5");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();

        let expected = vec![
            Token::Keyword(String::from("SELECT")),
            Token::Mult,
            Token::Keyword(String::from("FROM")),
            Token::Identifier(String::from("customer")),
            Token::Keyword(String::from("WHERE")),
            Token::Identifier(String::from("id")),
            Token::Eq,
            Token::Number(String::from("1")),
            Token::Keyword(String::from("LIMIT")),
            Token::Number(String::from("5")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn parse_simple_select() {
        let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT 5");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        //println!("AST = {:?}", ast);
        match ast {
            ASTNode::SQLSelect {
                projection, limit, ..
            } => {
                assert_eq!(3, projection.len());
                assert_eq!(Some(Box::new(ASTNode::SQLLiteralLong(5))), limit);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_projection_nested_type() {
        let sql = String::from("SELECT customer.address.state FROM foo");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        println!("{:?}", tokens);
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        println!("{:?}", ast);
    }

    #[test]
    fn parse_compound_expr_1() {
        use self::ASTNode::*;
        use self::SQLOperator::*;
        let sql = String::from("a + b * c");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        println!("AST = {:?}", ast);
        assert_eq!(
            SQLBinaryExpr {
                left: Box::new(SQLIdentifier("a".to_string())),
                op: Plus,
                right: Box::new(SQLBinaryExpr {
                    left: Box::new(SQLIdentifier("b".to_string())),
                    op: Multiply,
                    right: Box::new(SQLIdentifier("c".to_string()))
                })
            },
            ast
        );
    }

    #[test]
    fn parse_compound_expr_2() {
        use self::ASTNode::*;
        use self::SQLOperator::*;
        let sql = String::from("a * b + c");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        //println!("AST = {:?}", ast);
        assert_eq!(
            SQLBinaryExpr {
                left: Box::new(SQLBinaryExpr {
                    left: Box::new(SQLIdentifier("a".to_string())),
                    op: Multiply,
                    right: Box::new(SQLIdentifier("b".to_string()))
                }),
                op: Plus,
                right: Box::new(SQLIdentifier("c".to_string()))
            },
            ast
        );
    }

    #[test]
    fn parse_select_order_by() {
        let sql = String::from(
            "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC",
        );
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        //println!("AST = {:?}", ast);
        match ast {
            ASTNode::SQLSelect { order_by, .. } => {
                assert_eq!(
                    Some(vec![
                        ASTNode::SQLOrderBy {
                            expr: Box::new(ASTNode::SQLIdentifier("lname".to_string())),
                            asc: true,
                        },
                        ASTNode::SQLOrderBy {
                            expr: Box::new(ASTNode::SQLIdentifier("fname".to_string())),
                            asc: false,
                        },
                    ]),
                    order_by
                );
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_select_group_by() {
        let sql = String::from("SELECT id, fname, lname FROM customer GROUP BY lname, fname");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        //println!("AST = {:?}", ast);
        match ast {
            ASTNode::SQLSelect { group_by, .. } => {
                assert_eq!(
                    Some(vec![
                        ASTNode::SQLIdentifier("lname".to_string()),
                        ASTNode::SQLIdentifier("fname".to_string()),
                    ]),
                    group_by
                );
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_limit_accepts_all() {
        let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT ALL");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        //println!("AST = {:?}", ast);
        match ast {
            ASTNode::SQLSelect {
                projection, limit, ..
            } => {
                assert_eq!(3, projection.len());
                assert_eq!(None, limit);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_create_external_table() {
        let sql = String::from(
            "CREATE EXTERNAL TABLE uk_cities (\
             name VARCHAR(100) NOT NULL,\
             lat DOUBLE NOT NULL,\
             lng DOUBLE NOT NULL)",
        );

        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        //println!("AST = {:?}", ast);
        match ast {
            ASTNode::SQLCreateTable { name, columns, .. } => {
                assert_eq!("uk_cities", name);
                assert_eq!(3, columns.len());
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn parse_scalar_function_in_projection() {
        let sql = String::from("SELECT sqrt(id) FROM foo");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        //println!("AST = {:?}", ast);
        if let ASTNode::SQLSelect { projection, .. } = ast {
            assert_eq!(
                vec![
                    ASTNode::SQLFunction {
                        id: String::from("sqrt"),
                        args: vec![ASTNode::SQLIdentifier(String::from("id"))],
                    },
                ],
                projection
            );
        } else {
            assert!(false);
        }
    }

    fn compare(expected: Vec<Token>, actual: Vec<Token>) {
        //println!("------------------------------");
        //println!("tokens   = {:?}", actual);
        //println!("expected = {:?}", expected);
        //println!("------------------------------");
        assert_eq!(expected, actual);
    }

}
