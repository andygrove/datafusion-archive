#![feature(box_patterns)]

pub mod schema;
pub mod sql;
pub mod parser;
pub mod rel;
pub mod sqltorel;
pub mod exec;
pub mod csvrelation;


extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

