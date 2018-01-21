#![feature(box_patterns)]

pub mod sql;
pub mod parser;
pub mod rel;
pub mod sqltorel;
pub mod exec;

extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

extern crate csv;

