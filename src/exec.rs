use std::error::Error;

use super::rel::*;
use super::csvrelation::CsvRelation;

struct QueryExecutor {}

impl QueryExecutor {

    fn execute(plan: &Rel) -> Result<Box<Relation>,String> {
        match plan {
            &Rel::CsvFile { ref filename, ref schema } =>
                Ok(Box::new(CsvRelation::open(filename.to_string(), schema.clone()))),
            _ => Err("not implemented".to_string())
        }
    }

}


/// Evaluate a relational expression against a tuple
pub fn evaluate(tuple: &Tuple, tt: &TupleType, rex: &Rex) -> Result<Value, Box<Error>> {

    match rex {
        &Rex::BinaryExpr { box ref left, ref op, box ref right } => {
            let left_value = evaluate(tuple, tt, left)?;
            let right_value = evaluate(tuple, tt, right)?;
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

