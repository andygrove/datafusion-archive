#![feature(box_patterns)]

use std::error::Error;
use std::fs::File;

use super::schema::*;

/// A simple tuple implementation for testing and initial prototyping
#[derive(Debug)]
struct SimpleTuple {
    values: Vec<Value>
}

impl Tuple for SimpleTuple {

    fn get_value(&self, index: usize) -> Result<Value, Box<Error>> {
        Ok(self.values[index].clone())
    }

}
