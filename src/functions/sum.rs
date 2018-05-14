//! Sum() aggregate function

use super::super::errors::*;
use super::super::types::*;
use arrow::datatypes::*;
use std::rc::Rc;

pub struct SumFunction {
    value: ScalarValue,
    data_type: DataType,
}

impl SumFunction {
    pub fn new(dt: &DataType) -> Self {
        SumFunction {
            value: ScalarValue::Null,
            data_type: dt.clone(),
        }
    }
}

macro_rules! sum_of_column {
    ($SELF:ident, $BUF:ident, $VARIANT:ident) => {{
        for i in 0..$BUF.len() as usize {
            let value = *$BUF.get(i);
            match $SELF.value {
                ScalarValue::Null => $SELF.value = ScalarValue::$VARIANT(value),
                ScalarValue::$VARIANT(x) => $SELF.value = ScalarValue::$VARIANT(x + value),
                ref other => panic!(
                    "Type mismatch in SUM() for datatype {} - {:?}",
                    stringify!($VARIANT),
                    other
                ),
            }
        }
    }};
}

macro_rules! sum_of_scalar {
    ($SELF:ident, $VALUE:ident, $VARIANT:ident) => {{
        match $SELF.value {
            ScalarValue::Null => $SELF.value = ScalarValue::$VARIANT(*$VALUE),
            ScalarValue::$VARIANT(x) => $SELF.value = ScalarValue::$VARIANT(x + *$VALUE),
            _ => panic!("type mismatch :("),
        }
    }};
}

impl AggregateFunction for SumFunction {
    fn name(&self) -> String {
        "SUM".to_string()
    }

    fn args(&self) -> Vec<Field> {
        vec![Field::new("arg", DataType::UInt32, true)]
    }

    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn execute(&mut self, args: &Vec<Value>) -> Result<()> {
        assert_eq!(1, args.len());
        match args[0] {
            Value::Column(ref array) => match array.data() {
                ArrayData::UInt8(ref buf) => sum_of_column!(self, buf, UInt8),
                ArrayData::UInt16(ref buf) => sum_of_column!(self, buf, UInt16),
                ArrayData::UInt32(ref buf) => sum_of_column!(self, buf, UInt32),
                ArrayData::UInt64(ref buf) => sum_of_column!(self, buf, UInt64),
                ArrayData::Float32(ref buf) => sum_of_column!(self, buf, Float32),
                ArrayData::Float64(ref buf) => sum_of_column!(self, buf, Float64),
                ArrayData::Int8(ref buf) => sum_of_column!(self, buf, Int8),
                ArrayData::Int16(ref buf) => sum_of_column!(self, buf, Int16),
                ArrayData::Int32(ref buf) => sum_of_column!(self, buf, Int32),
                ArrayData::Int64(ref buf) => sum_of_column!(self, buf, Int64),
                ArrayData::Utf8(_) => unimplemented!("Not done for this type: Utf8"),
                ArrayData::Boolean(_) => unimplemented!("Not done for this type: Bool"),
                ArrayData::Struct(_) => unimplemented!("Not done for this type: Struct"),
            },
            Value::Scalar(ref v) => match v.as_ref() {
                ScalarValue::UInt8(ref value) => sum_of_scalar!(self, value, UInt8),
                ScalarValue::UInt16(ref value) => sum_of_scalar!(self, value, UInt16),
                ScalarValue::UInt32(ref value) => sum_of_scalar!(self, value, UInt32),
                ScalarValue::UInt64(ref value) => sum_of_scalar!(self, value, UInt64),
                ScalarValue::Float32(ref value) => sum_of_scalar!(self, value, Float32),
                ScalarValue::Float64(ref value) => sum_of_scalar!(self, value, Float64),
                _ => unimplemented!("Not done for type"),
            },
        }
        Ok(())
    }

    fn finish(&self) -> Result<Value> {
        Ok(Value::Scalar(Rc::new(self.value.clone())))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_sum() {
        let mut sum = SumFunction::new(&DataType::UInt8);
        assert_eq!(DataType::UInt8, sum.return_type());
        let values: Vec<u8> = vec![12, 22, 32, 6, 58];

        sum.execute(&vec![Value::Column(Rc::new(Array::from(values)))])
            .unwrap();
        let result = sum.finish().unwrap();

        match result {
            Value::Scalar(ref v) => assert_eq!(v.get_u8().unwrap(), 130),
            _ => panic!(),
        }
    }

    #[test]
    fn test_sum_f64() {
        let mut sum = SumFunction::new(&DataType::Float64);
        assert_eq!(DataType::Float64, sum.return_type());
        let values: Vec<f64> = vec![1.1, 2.2, 3.3, 4.4, 5.5];

        sum.execute(&vec![Value::Column(Rc::new(Array::from(values)))])
            .unwrap();
        let result = sum.finish().unwrap();

        match result {
            Value::Scalar(ref v) => assert_eq!(v.get_f64().unwrap(), 16.5),
            _ => panic!(),
        }
    }
}
