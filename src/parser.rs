use std::error::Error;
use std::fs::File;

use super::sql::*;

//pub enum Keyword {
//    Select,
//    From,
//    Where
//}

pub enum Token {
    Identifier(String),
    Keyword(String),
    Operator(String),
}

#[derive(Debug,Clone)]
pub enum ParserError {
    TokenizerError(String)
}

struct Tokenizer {
    query: String
}

impl Tokenizer {

    pub fn tokenize(&mut self) -> Result<Vec<Token>, ParserError> {

        let mut peekable = self.query.chars().peekable();

        Err(ParserError::TokenizerError(String::from("not implemented yet")))
    }

}


