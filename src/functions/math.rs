use std::rc::Rc;

use super::super::types::*;

use arrow::array::*;
use arrow::datatypes::*;

pub struct SqrtFunction {}

impl ScalarFunction for SqrtFunction {
    fn name(&self) -> String {
        "sqrt".to_string()
    }

    fn execute(&self, args: Vec<Rc<Value>>) -> Result<Rc<Value>, ExecutionError> {
        match args[0].as_ref() {
            &Value::Column(ref arr) => {
                match arr.data() {
                    &ArrayData::Float32(ref v) => Ok(Rc::new(Value::Column(Rc::new(Array::from(
                        v.iter().map(|v| v.sqrt()).collect::<Vec<f32>>(),
                    ))))),
                    &ArrayData::Float64(ref v) => Ok(Rc::new(Value::Column(Rc::new(Array::from(
                        v.iter().map(|v| v.sqrt()).collect::<Vec<f64>>(),
                    ))))),
                    &ArrayData::Int32(ref v) => Ok(Rc::new(Value::Column(Rc::new(Array::from(
                        v.iter().map(|v| (v as f64).sqrt()).collect::<Vec<f64>>(),
                    ))))),
                    &ArrayData::Int64(ref v) => Ok(Rc::new(Value::Column(Rc::new(Array::from(
                        v.iter().map(|v| (v as f64).sqrt()).collect::<Vec<f64>>(),
                    ))))),
                    _ => Err(ExecutionError::Custom(
                        "Unsupported arg type for sqrt".to_string(),
                    )),
                }
            }
            _ => Err(ExecutionError::Custom(
                "Unsupported arg type for sqrt".to_string(),
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
        DataType::Float64
    }
}
