use std::rc::Rc;

use super::super::api::*;
use super::super::arrow::*;

/// create a point from two doubles
pub struct STPointFunc;

impl ScalarFunction for STPointFunc {

    fn name(&self) -> String {
        "ST_Point".to_string()
    }


    fn execute(&self, args: Vec<Rc<Array>>) -> Result<Rc<Array>,Box<String>> {
        if args.len() != 2 {
            return Err(Box::new("Wrong argument count for ST_Point".to_string()))
        }
        match (args[0].as_ref(), args[1].as_ref()) {
            (&Array::Float64(_), &Array::Float64(_)) =>
                Ok(Rc::new(Array::Struct(vec![args[0].clone(), args[1].clone()]))),
            _ => Err(Box::new("Unsupported type for ST_Point".to_string()))
        }
    }

    fn args(&self) -> Vec<Field> {
        vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false)
        ]
    }

    fn return_type(&self) -> DataType {
        DataType::Struct(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false)
        ])
    }
}

/// create a point from two doubles
pub struct STAsText;

impl ScalarFunction for STAsText {

    fn name(&self) -> String {
        "ST_AsText".to_string()
    }

    fn execute(&self, args: Vec<Rc<Array>>) -> Result<Rc<Array>,Box<String>> {
        if args.len() != 1 {
            return Err(Box::new("Wrong argument count for ST_AsText".to_string()))
        }
        match args[0].as_ref() {
            &Array::Struct(ref fields) => match (fields[0].as_ref(), fields[1].as_ref()) {
                (&Array::Float64(ref lat), &Array::Float64(ref lon)) => {
                    Ok(Rc::new(Array::Utf8(
                        lat.iter().zip(lon.iter())
                            .map(|(lat2,lon2)| format!("POINT ({} {})", lat2, lon2))
                            .collect())))
                },
                _ => Err(Box::new("Unsupported type for ST_AsText".to_string()))
            },
            _ => Err(Box::new("Unsupported type for ST_AsText".to_string()))
        }
    }

    fn args(&self) -> Vec<Field> {
        vec![
            Field::new("point", DataType::Struct(vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false)
            ]), false)
        ]
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }
}



