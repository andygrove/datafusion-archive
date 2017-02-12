use std::error::Error;
use std::fs::File;

use super::schema::*;
use super::plan::*;

///// A simple tuple implementation for testing and initial prototyping
//#[derive(Debug)]
//struct SimpleTuple {
//    values: Vec<Value>
//}
//
//impl Tuple for SimpleTuple {
//
//    fn get_value(&self, index: usize) -> Result<Value, Box<Error>> {
//        Ok(self.values[index].clone())
//    }
//
//}

pub fn evaluate(tuple: &Tuple, tt: &TupleType, Rex: &Rex) -> Result<Value, Box<Error>> {

    match Rex {
        &Rex::BinaryExpr { box ref left, ref op, box ref right } => {
            //TODO: remove use of unwrap() here
            let left_value = evaluate(tuple, tt, left).unwrap();
            let right_value = evaluate(tuple, tt, right).unwrap();
            match op {
                &Operator::Eq => Ok(Value::Boolean(left_value == right_value)),
                &Operator::NotEq => Ok(Value::Boolean(left_value != right_value)),
                &Operator::Lt => Ok(Value::Boolean(left_value < right_value)),
                &Operator::LtEq => Ok(Value::Boolean(left_value <= right_value)),
                &Operator::Gt => Ok(Value::Boolean(left_value > right_value)),
                &Operator::GtEq => Ok(Value::Boolean(left_value >= right_value)),
            }
        },
        &Rex::TupleValue(index) => Ok(tuple.values[index].clone()),
        &Rex::Literal(ref value) => Ok(value.clone()),
    }

}

