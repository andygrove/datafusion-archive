use super::super::api::*;
use super::super::rel::*;

/// create a latitude/longitude value from two doubles
pub struct LatLngFunc {
}

impl ScalarFunction for LatLngFunc {
    fn execute(&self, args: Vec<Value>) -> Result<Value,Box<String>> {
        if args.len() != 2 {
            return Err(Box::new("Wrong argument count for LatLngFunc".to_string()))
        }
        match (&args[0], &args[1]) {
            (&Value::Double(lat), &Value::Double(lng)) => Ok(Value::ComplexValue(
                vec![Value::Double(lat), Value::Double(lng)])),
            _ => Err(Box::new("Unsupported type for LatLngFunc".to_string()))
        }
    }
}



