use std::convert::From;
use std::rc::Rc;

use arrow::array::*;
use arrow::datatypes::*;

use super::super::types::*;

/// create a point from two doubles
pub struct STPointFunc;

impl ScalarFunction for STPointFunc {
    fn name(&self) -> String {
        "ST_Point".to_string()
    }

    fn execute(&self, args: Vec<Rc<Value>>) -> Result<Rc<Value>, ExecutionError> {
        if args.len() != 2 {
            return Err(ExecutionError::Custom(
                "Wrong argument count for ST_Point".to_string(),
            ));
        }
        match (args[0].as_ref(), args[1].as_ref()) {
            (&Value::Column(ref arr1), &Value::Column(ref arr2)) => {
                let field = Rc::new(Field::new(&self.name(), self.return_type(), false));
                match (arr1.data(), arr2.data()) {
                    (&ArrayData::Float64(_), &ArrayData::Float64(_)) => {
                        let nested: Vec<Rc<Array>> = vec![arr1.clone(), arr2.clone()];
                        let new_array = Array::new(arr1.len() as usize, ArrayData::Struct(nested));
                        Ok(Rc::new(Value::Column(Rc::new(new_array))))
                    }
                    _ => Err(ExecutionError::Custom(
                        "Unsupported type for ST_Point".to_string(),
                    )),
                }
            }
            _ => Err(ExecutionError::Custom(
                "Unsupported type for ST_Point".to_string(),
            )),
        }
    }

    fn args(&self) -> Vec<Field> {
        vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]
    }

    fn return_type(&self) -> DataType {
        DataType::Struct(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ])
    }
}

/// create a point from two doubles
pub struct STAsText;

impl ScalarFunction for STAsText {
    fn name(&self) -> String {
        "ST_AsText".to_string()
    }

    fn execute(&self, args: Vec<Rc<Value>>) -> Result<Rc<Value>, ExecutionError> {
        if args.len() != 1 {
            return Err(ExecutionError::Custom(
                "Wrong argument count for ST_AsText".to_string(),
            ));
        }
        match args[0].as_ref() {
            &Value::Column(ref arr) => match arr.data() {
                &ArrayData::Struct(ref fields) => {
                    match (fields[0].as_ref().data(), fields[1].as_ref().data()) {
                        (&ArrayData::Float64(ref lat), &ArrayData::Float64(ref lon)) => {
                            //                        println!("lat.len() = {}, lng.len = {}", lat.len(), lon.len());

                            let wkt: Vec<String> = lat.iter()
                                .zip(lon.iter())
                                .map(|(lat2, lon2)| format!("POINT ({} {})", lat2, lon2))
                                .collect();
                            Ok(Rc::new(Value::Column(
                                Rc::new(Array::from(wkt)),
                            )))
                        }
                        _ => Err(ExecutionError::Custom(
                            "Unsupported type for ST_AsText".to_string(),
                        )),
                    }
                }
                _ => Err(ExecutionError::Custom(
                    "Unsupported type for ST_AsText".to_string(),
                )),
            },
            _ => Err(ExecutionError::Custom(
                "Unsupported type for ST_AsText".to_string(),
            )),
        }
    }

    fn args(&self) -> Vec<Field> {
        vec![
            Field::new(
                "point",
                DataType::Struct(vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ]),
                false,
            ),
        ]
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }
}
