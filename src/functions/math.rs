use std::rc::Rc;

use super::super::api::*;
use super::super::arrow::{DataType, Field, Array, ArrayData};
use super::super::exec::Value;

pub struct SqrtFunction {
}

impl ScalarFunction for SqrtFunction {

    fn name(&self) -> String {
        "sqrt".to_string()
    }

    fn execute(&self, args: Vec<Rc<Value>>) -> Result<Rc<Value>,Box<String>> {
        match args[0].as_ref() {
            &Value::Column(_, ref arr)=> {
                let field = Rc::new(Field::new(&self.name(), self.return_type(), false));
                match arr.data() {
                    &ArrayData::Float32(ref v) => Ok(Rc::new(Value::Column(field, Rc::new(Array::new(ArrayData::Float32(v.iter().map(|v| v.sqrt()).collect())))))),
                    &ArrayData::Float64(ref v) => Ok(Rc::new(Value::Column(field, Rc::new(Array::new(ArrayData::Float64(v.iter().map(|v| v.sqrt()).collect())))))),
                    &ArrayData::Int32(ref v) => Ok(Rc::new(Value::Column(field, Rc::new(Array::new(ArrayData::Float64(v.iter().map(|v| (*v as f64).sqrt()).collect())))))),
                    &ArrayData::Int64(ref v) => Ok(Rc::new(Value::Column(field, Rc::new(Array::new(ArrayData::Float64(v.iter().map(|v| (*v as f64).sqrt()).collect())))))),
                    _ => Err(Box::new("Unsupported arg type for sqrt".to_string()))
                }
            },
            _ => Err(Box::new("Unsupported arg type for sqrt".to_string()))
        }
    }

    fn args(&self) -> Vec<Field> {
        vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false)
        ]
    }

    fn return_type(&self) -> DataType {
        DataType::Float64
    }
}



