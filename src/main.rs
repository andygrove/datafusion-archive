#![feature(box_patterns)]

use std::error::Error;
use std::fs::File;

mod schema;
use schema::*;

mod plan;
use plan::*;

mod csvrelation;
use csvrelation::*;

//
//
//fn evaluate(tuple: &Tuple, tt: &TupleType, expr: &Expr) -> Result<Value, Box<std::error::Error>> {
//
//    match expr {
//        &Expr::BinaryExpr { box ref left, ref op, box ref right } => {
//            //TODO: remove use of unwrap() here
//            let left_value = evaluate(tuple, tt, left).unwrap();
//            let right_value = evaluate(tuple, tt, right).unwrap();
//            match op {
//                &Operator::Eq => Ok(Value::Boolean(left_value == right_value)),
//                &Operator::NotEq => Ok(Value::Boolean(left_value != right_value)),
//                &Operator::Lt => Ok(Value::Boolean(left_value < right_value)),
//                &Operator::LtEq => Ok(Value::Boolean(left_value <= right_value)),
//                &Operator::Gt => Ok(Value::Boolean(left_value > right_value)),
//                &Operator::GtEq => Ok(Value::Boolean(left_value >= right_value)),
//            }
//        },
//        &Expr::TupleValue(index) => tuple.get_value(index),
//        &Expr::Literal(ref value) => Ok(value.clone()),
//    }
//
//}
//
//trait TupleConsumer {
//    fn process(&self, tuple: &Tuple);
//}
//
//struct DebugConsumer {
//    tuple_type: TupleType,
//}
//
//impl TupleConsumer for DebugConsumer {
//    fn process(&self, tuple: &Tuple) {
//        print_tuple(&self.tuple_type, tuple);
//    }
//}
//
//struct FilterConsumer<'a> {
//    tuple_type: TupleType,
//    filter_expr: Expr,
//    next_consumer: Option<&'a TupleConsumer>
//}
//
//impl<'a> TupleConsumer for FilterConsumer<'a> {
//    fn process(&self, tuple: &Tuple) {
//        match evaluate(tuple, &self.tuple_type, &self.filter_expr) {
//            Ok(v) => match v {
//                Value::Boolean(b) => {
//                    if (b) {
//                        match self.next_consumer {
//                            Some(c) => c.process(tuple),
//                            None => {}
//                        }
//                    }
//                },
//                //TODO: this should be an error - filter expressions should return boolean
//                _ => {}
//            },
//            _ => {}
//        }
//    }
//}


fn main() {

    // define schema for data source (csv file)
    let tt = TupleType {
        columns: vec![
            ColumnMeta { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnMeta { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    // open csv file
    let mut csv = CsvRelation::open(String::from("people.csv"), tt.clone());

    let mut it = csv.scan();

    while let Some(t) = it.next() {
        println!("{:?}", t);
    }

//    // create simple filter expression for "id = 2"
//    let filter_expr = Expr::BinaryExpr {
//        left: Box::new(Expr::TupleValue(0)),
//        op: Operator::Eq,
//        right: Box::new(Expr::Literal(Value::UnsignedLong(2)))
//    };
//
//    let debug_consumer = DebugConsumer { tuple_type: tt.clone() };
//
//    let filter_consumer = FilterConsumer {
//        tuple_type: tt.clone(),
//        filter_expr: filter_expr,
//        next_consumer: Some(&debug_consumer)
//    };
//
//    csv.scan(&filter_consumer);
}
