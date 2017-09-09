use std::iter::Peekable;
use std::str::Chars;
use std::ascii::AsciiExt;

use super::sql::*;

#[derive(Debug,Clone,PartialEq)]
pub enum Token {
    Identifier(String),
    Keyword(String),
    Operator(String),
    Number(String),
    Comma,
    Whitespace,
    Eq,
    Neq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    Plus,
    Minus,
    Mult,
    Div,
    LParen,
    RParen,

    //Operator(String)
}

#[derive(Debug,Clone)]
pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
}

struct Tokenizer {
    query: String,
}

impl Tokenizer {

    pub fn tokenize(&mut self) -> Result<Vec<Token>, ParserError> {

        let mut peekable = self.query.chars().peekable();

        let mut tokens : Vec<Token> = vec![];

        while let Some(token) = self.next_token(&mut peekable)? {
            tokens.push(token);
        }

        Ok(tokens.into_iter().filter(|t| match t {
            &Token::Whitespace => false,
            _ => true
        }).collect())
    }

    fn next_token(&self, chars: &mut Peekable<Chars>) -> Result<Option<Token>, ParserError> {
        match chars.peek() {
            Some(&ch) => match ch {
                // whitespace
                ' ' | '\t' | '\n' => {
                    chars.next(); // consume
                    Ok(Some(Token::Whitespace))
                },
                // identifier or keyword
                'a' ... 'z' | 'A' ... 'Z' | '_' | '@' => {
                    let mut s = String::new();
                    while let Some(&ch) = chars.peek() {
                        match ch {
                            'a' ... 'z' | 'A' ... 'Z' | '_' | '0' ... '9' => {
                                chars.next(); // consume
                                s.push(ch);
                            },
                            _ => break
                        }
                    }
                    match s.to_uppercase().as_ref() {
                        "SELECT" | "FROM" | "WHERE" | "LIMIT" | "ORDER" | "GROUP" | "BY" |
                        "UNION" | "ALL"| "UPDATE" | "DELETE" | "IN" | "NOT" | "NULL" |
                        "SET" => Ok(Some(Token::Keyword(s))),
                        _ => Ok(Some(Token::Identifier(s))),
                    }
                },
                // numbers
                '0' ... '9' => {
                    let mut s = String::new();
                    while let Some(&ch) = chars.peek() {
                        match ch {
                            '0' ... '9' => {
                                chars.next(); // consume
                                s.push(ch);
                            },
                            _ => break
                        }
                    }
                    Ok(Some(Token::Number(s)))
                },
                // punctuation
                ',' => { chars.next(); Ok(Some(Token::Comma)) },
                '(' => { chars.next(); Ok(Some(Token::LParen)) },
                ')' => { chars.next(); Ok(Some(Token::RParen)) },
                // operators
                '+' => { chars.next(); Ok(Some(Token::Plus)) },
                '-' => { chars.next(); Ok(Some(Token::Minus)) },
                '*' => { chars.next(); Ok(Some(Token::Mult)) },
                '/' => { chars.next(); Ok(Some(Token::Div)) },
                '=' => { chars.next(); Ok(Some(Token::Eq)) },
                '<' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some(&ch) => match ch {
                            '=' => {
                                chars.next();
                                Ok(Some(Token::LtEq))
                            },
                            '>' => {
                                chars.next();
                                Ok(Some(Token::Neq))
                            },
                            _ => Ok(Some(Token::Lt))
                        },
                        None => Ok(Some(Token::Lt))
                    }
                },
                '>' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some(&ch) => match ch {
                            '=' => {
                                chars.next();
                                Ok(Some(Token::GtEq))
                            },
                            _ => Ok(Some(Token::Gt))
                        },
                        None => Ok(Some(Token::Gt))
                    }
                },
                _ => Err(ParserError::TokenizerError(
                    String::from(format!("unhandled char '{}' in tokenizer", ch))))
            },
            None => Ok(None)
        }
    }
}

pub struct Parser {
    tokens: Vec<Token>,
    index: usize
}

impl Parser {

    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens: tokens, index: 0 }
    }

    pub fn parse(&mut self) -> Result<ASTNode, ParserError> {
        self.parse_expr(0)
    }

    fn parse_expr(&mut self, precedence: u8) -> Result<ASTNode, ParserError> {

        let mut expr = self.parse_prefix()?;

        while let Some(tok) = self.peek_token() {

            let next_precedence = self.get_precedence(&tok)?;
            if precedence >= next_precedence {
                break;
            }

            if let Some(new_expr) = self.parse_infix(expr.clone(), next_precedence)? {
                expr = new_expr;
            }
        }

        Ok(expr)
    }

    fn parse_prefix(&mut self) -> Result<ASTNode, ParserError> {
        match self.next_token() {
            Some(t) => {
                match t {
                    Token::Keyword(k) => {
                        match k.to_uppercase().as_ref() {
                            "SELECT" => Ok(self.parse_select()?),
                            _ => Err(ParserError::ParserError(
                                format!("No prefix parser for keyword {}", k))),
                        }
                    },
                    Token::Identifier(id) =>
                        Ok(ASTNode::SQLIdentifier { id: id, parts: vec![] }),
                    Token::Number(n) =>
                        Ok(ASTNode::SQLLiteralInt(0)), //TODO: parse the number
                    _ => Err(ParserError::ParserError(
                        format!("Prefix parser expected a keyword but found {:?}", t)))
                }
            },
            None => Err(ParserError::ParserError(
                format!("Prefix parser expected a keyword but hit EOF")))
        }
    }

    fn parse_infix(&mut self, expr: ASTNode, precedence: u8) -> Result<Option<ASTNode>, ParserError> {
        match self.next_token() {
            Some(tok) => {
                match tok {
                    Token::Eq => Ok(Some(ASTNode::SQLBinaryExpr {
                        left: Box::new(expr),
                        op: SQLOperator::EQ,
                        right: Box::new(self.parse_expr(precedence)?)
                    })),
                    _ => Err(ParserError::ParserError(
                        format!("No infix parser for token {:?}", tok))),
                }
            },
            None => Ok(None)
        }
    }

    fn get_precedence(&self, tok: &Token) -> Result<u8, ParserError> {
        match tok {
            &Token::Eq | &Token::Lt | & Token::LtEq |
            &Token::Neq | &Token::Gt | & Token::GtEq => Ok(20),
            &Token::Plus | &Token::Minus => Ok(30),
            &Token::Mult | &Token::Div => Ok(40),
            _ => Ok(0)
                /*Err(ParserError::TokenizerError(
                format!("invalid token {:?} for get_precedence", tok)))*/
        }
    }

    fn peek_token(&mut self) -> Option<Token> {
        if self.index < self.tokens.len() {
            Some(self.tokens[self.index].clone())
        } else {
            None
        }
    }

    fn next_token(&mut self) -> Option<Token> {
        if self.index < self.tokens.len() {
            self.index = self.index + 1;
            Some(self.tokens[self.index-1].clone())
        } else {
            None
        }
    }

//    fn next_keyword(&mut self) -> Result<Option<Token>, Err> {
//        match self.next_token()? {
//            Some(t) => match t {
//                Token::Keyword => Ok(Some(t)),
//                _ => Err(ParserError::ParserError(
//                    format!("Expected keyword, found {:?}", t)))
//            },
//            None => Ok(None)
//        }
//    }

    fn parse_keyword(&mut self, expected: &'static str) -> bool {
        match self.peek_token() {
            Some(Token::Keyword(k)) => {
                if expected.eq_ignore_ascii_case(k.as_str()) {
                    self.next_token();
                    true
                } else {
                    false
                }
            },
            _ => false
        }
    }

//    fn parse_identifier(&mut self) -> Result<ASTNode::SQLIdentifier, Err> {
//        let expr = self.parse_expr()?;
//        match expr {
//            Some(ASTNode::SQLIdentifier { .. }) => Ok(expr),
//            _ => Err(ParserError::ParserError(format!("Expected identifier but found {:?}", expr)))
//        }
//    }

    // specific methods

    fn parse_select(&mut self) -> Result<ASTNode, ParserError> {

        let projection = self.parse_expr_list()?;

        let relation : Option<Box<ASTNode>> = if self.parse_keyword("FROM") {
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

        //TODO: parse GROUP BY
        //TODO: parse HAVING
        //TODO: parse ORDER BY
        //TODO: parse LIMIT

        if let Some(next_token) = self.peek_token() {
            Err(ParserError::ParserError(format!("Unexpected token at end of SELECT: {:?}", next_token)))
        } else {
            Ok(ASTNode::SQLSelect {
                projection: projection,
                selection: selection,
                relation: relation,
                limit: None,
                order: None,
            })
        }
    }

    fn parse_expr_list(&mut self) -> Result<Vec<ASTNode>, ParserError> {
        println!("parse_expr_list()");
        let mut expr_list : Vec<ASTNode> = vec![];
        loop {
            println!("parse_expr_list() top of loop");
            expr_list.push(self.parse_expr(0)?);
            if let Some(t) = self.peek_token() {
                if t == Token::Comma {
                    self.next_token();
                } else {
                    println!("parse_expr_list() BREAK on token={:?}", t);
                    break;
                }
            }
        }
        Ok(expr_list)
    }

}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn tokenize_select_1()  {
        let sql = String::from("SELECT 1");
        let mut tokenizer = Tokenizer { query: sql };
        let tokens = tokenizer.tokenize().unwrap();
        println!("tokens = {:?}", tokens);
        assert_eq!(2, tokens.len());
        assert_eq!(Token::Keyword(String::from("SELECT")), tokens[0]);
        assert_eq!(Token::Number(String::from("1")), tokens[1]);
    }

    #[test]
    fn tokenize_simple_select()  {
        let sql = String::from("SELECT * FROM customer WHERE id = 1");
        let mut tokenizer = Tokenizer { query: sql };
        let tokens = tokenizer.tokenize().unwrap();
        println!("tokens = {:?}", tokens);
        assert_eq!(8, tokens.len());
        assert_eq!(Token::Keyword(String::from("SELECT")), tokens[0]);
        assert_eq!(Token::Mult, tokens[1]);
        assert_eq!(Token::Keyword(String::from("FROM")), tokens[2]);
        assert_eq!(Token::Identifier(String::from("customer")), tokens[3]);
        assert_eq!(Token::Keyword(String::from("WHERE")), tokens[4]);
        assert_eq!(Token::Identifier(String::from("id")), tokens[5]);
        assert_eq!(Token::Eq, tokens[6]);
        assert_eq!(Token::Number(String::from("1")), tokens[7]);
    }

    #[test]
    fn parse_simple_select() {
        let sql = String::from("SELECT id, fname, lname FROM customer WHERE id = 1");
        let mut tokenizer = Tokenizer { query: sql };
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens);
        let ast = parser.parse().unwrap();
        println!("AST = {:?}", ast);
        match ast {
            ASTNode::SQLSelect { projection, .. } => {
                assert_eq!(3, projection.len());
            },
            _ => assert!(false)
        }
    }
}

