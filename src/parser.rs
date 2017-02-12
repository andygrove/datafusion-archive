use std::error::Error;
use std::fs::File;
use std::iter::Peekable;
use std::str::Chars;

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
    Whitespace
}

#[derive(Debug,Clone)]
pub enum ParserError {
    TokenizerError(String)
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

        Ok(tokens)
    }

    fn next_token(&self, chars: &mut Peekable<Chars>) -> Result<Option<Token>, ParserError> {
        match chars.peek() {
            Some(&ch) => match ch {
                ' ' | '\t' | '\n' => {
                    chars.next();
                    Ok(Some(Token::Whitespace))
                },
                _ => Err(ParserError::TokenizerError(String::from("unhandled case in tokenizer")))
            },
            None => Ok(None)
        }
    }

}


