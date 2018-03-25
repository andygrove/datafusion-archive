use std::rc::Rc;

use super::super::api::*;
use super::super::arrow::*;

/// create a point from two doubles
pub struct STPointFunc;

impl ScalarFunction for STPointFunc {

    fn name(&self) -> String {
        "ST_Point".to_string()
    }


    fn execute(&self, args: Vec<Rc<ColumnData>>) -> Result<Rc<ColumnData>,Box<String>> {
        if args.len() != 2 {
            return Err(Box::new("Wrong argument count for ST_Point".to_string()))
        }
        match (args[0].as_ref(), args[1].as_ref()) {
            (&ColumnData::Double(_), &ColumnData::Double(_)) =>
                Ok(Rc::new(ColumnData::ComplexValue(vec![args[0].clone(), args[1].clone()]))),
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

/// create a point from two doubles
pub struct STAsText;

impl ScalarFunction for STAsText {

    fn name(&self) -> String {
        "ST_AsText".to_string()
    }

    fn execute(&self, args: Vec<Rc<ColumnData>>) -> Result<Rc<ColumnData>,Box<String>> {
        if args.len() != 1 {
            return Err(Box::new("Wrong argument count for ST_AsText".to_string()))
        }
        match args[0].as_ref() {
            &ColumnData::ComplexValue(ref fields) => match (fields[0].as_ref(), fields[1].as_ref()) {
                (&ColumnData::Double(ref lat), &ColumnData::Double(ref lon)) => {
                    Ok(Rc::new(ColumnData::String(
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
            Field::new("point", DataType::ComplexType(vec![
                Field::new("x", DataType::Double, false),
                Field::new("y", DataType::Double, false)
            ]), false)
        ]
    }

    fn return_type(&self) -> DataType {
        DataType::String
    }
}



