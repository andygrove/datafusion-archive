use super::super::api::*;
use super::super::rel::*;

pub struct SqrtFunction {
}

impl ScalarFunction for SqrtFunction {
    fn execute(&self, args: Vec<Value>) -> Result<Value,Box<String>> {
        match args[0] {
            Value::Double(d) => Ok(Value::Double(d.sqrt())),
            Value::UnsignedLong(l) => Ok(Value::Double((l as f64).sqrt())),
            _ => Err(Box::new("Unsupported arg type for sqrt".to_string()))
        }
    }
}



