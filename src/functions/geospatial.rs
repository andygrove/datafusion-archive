use super::super::api::*;
use super::super::rel::*;

/// create a point from two doubles
pub struct STPointFunc;

impl ScalarFunction for STPointFunc {

    fn name(&self) -> String {
        "ST_Point".to_string()
    }


    fn execute(&self, args: Vec<Value>) -> Result<Value,Box<String>> {
        if args.len() != 2 {
            return Err(Box::new("Wrong argument count for ST_Point".to_string()))
        }
        match (&args[0], &args[1]) {
            (&Value::Double(lat), &Value::Double(lng)) => Ok(Value::ComplexValue(
                vec![Value::Double(lat), Value::Double(lng)])),
            _ => Err(Box::new("Unsupported type for ST_Point".to_string()))
        }
    }

    fn args(&self) -> Vec<Field> {
        vec![
            Field::new("x", DataType::Double, false),
            Field::new("y", DataType::Double, false)
        ]
    }

    fn return_type(&self) -> DataType {
        DataType::ComplexType(vec![
            Field::new("x", DataType::Double, false),
            Field::new("y", DataType::Double, false)
        ])
    }
}

/// perform a mercator sphere projection on a LatLng
pub struct MercatorProjection;



