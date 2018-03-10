use super::super::api::*;
use super::super::rel::*;

pub struct SqrtFunction {
}

impl ScalarFunction for SqrtFunction {

    fn name(&self) -> String {
        "sqrt".to_string()
    }

    fn execute(&self, args: Vec<&Value>) -> Result<Value,Box<String>> {
        match args[0] {
            &Value::Double(d) => Ok(Value::Double(d.sqrt())),
            &Value::Long(l) => Ok(Value::Double((l as f64).sqrt())),
            _ => Err(Box::new("Unsupported arg type for sqrt".to_string()))
        }
    }

    fn args(&self) -> Vec<Field> {
        vec![
            Field::new("x", DataType::Double, false),
            Field::new("y", DataType::Double, false)
        ]
    }

    fn return_type(&self) -> DataType {
        DataType::Double
    }
}



