use std::error::Error;

use super::rel::*;
use super::csvrelation::CsvRelation;

struct QueryExecutor {}

impl QueryExecutor {

    fn execute(&self, plan: &Rel) -> Result<Box<Relation>,String> {
        match plan {
            &Rel::CsvFile { ref filename, ref schema } =>
                Ok(Box::new(CsvRelation::open(filename.to_string(), schema.clone()))),

            &Rel::Selection { ref expr, box ref input } => {
//                let input_rel = self.execute(&input)?;
//                Ok(Box::new(FilterRelation {
//                    input: input_rel,
//                    schema: input_rel.schema().clone()
//                }))
                unimplemented!("selection")
            },

            _ => Err("not implemented".to_string())
        }
    }

}


struct FilterRelation<'a> {
    schema: TupleType,
    input: Box<Relation<'a>>
}

impl<'a> Relation<'a> for FilterRelation<'a> {
    fn schema(&'a self) -> TupleType {
        unimplemented!()
    }

    fn scan(&'a mut self) -> Box<Iterator<Item=Tuple> + 'a> {
        unimplemented!()
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

