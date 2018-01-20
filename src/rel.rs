/// The data types supported by this database. Currently just u64 and string but others
/// will be added later, including complex types
#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum DataType {
    UnsignedLong,
    String,
    Double
}

/// Definition of a column in a relation (data set).
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool
}

/// Definition of a relation (data set) consisting of one or more columns.
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct TupleType {
    pub columns: Vec<ColumnMeta>
}


/// A tuple represents one row within a relation and is implemented as a trait to allow for
/// specific implementations for different data sources
//pub trait Tuple {
//    fn get_value(&self, index: usize) -> Result<Value, Box<Error>>;
//}

#[derive(Debug,Clone)]
pub struct Tuple {
    pub values: Vec<Value>
}

pub trait Relation<'a> {
    fn schema(&'a self) -> TupleType;
    fn scan(&'a mut self) -> Box<Iterator<Item=Tuple> + 'a>;
}

pub trait DataFrame<T> {
    fn select(&mut self, column_names: Vec<String>) -> Box<Self>;
}




/// Value holder for all supported data types
#[derive(Debug,Clone,PartialEq,PartialOrd,Serialize,Deserialize)]
pub enum Value {
    UnsignedLong(u64),
    String(String),
    Boolean(bool),
    Double(f64)
}

#[derive(Debug,Serialize,Deserialize)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// Relation Expression
#[derive(Debug,Serialize, Deserialize)]
pub enum Rex {
    /// index into a value within the tuple
    TupleValue(usize),
    /// literal value
    Literal(Value),
    /// binary expression e.g. "age > 21"
    BinaryExpr { left: Box<Rex>, op: Operator, right: Box<Rex> },
}

/// Relations
#[derive(Debug,Serialize, Deserialize)]
pub enum Rel {
    Projection { expr: Vec<Rex>, input: Option<Box<Rel>> },
    Selection { expr: Rex, input: Box<Rel> },
    TableScan { schema: String, table: String },
    CsvFile { filename: String, schema: TupleType },
}

#[cfg(test)]
mod tests {

    use super::*;
    use super::Rel::*;
    use super::Rex::*;
    use super::Value::*;
    extern crate serde_json;

    #[test]
    fn serde() {

        let tt = TupleType {
            columns: vec![
                ColumnMeta { name: "id".to_string(), data_type: DataType::UnsignedLong, nullable: false },
                ColumnMeta { name: "name".to_string(), data_type: DataType::String, nullable: false }
            ]
        };

        let csv = CsvRelation { filename: "test/people.csv".to_string(), schema: tt };

        let filter_expr = BinaryExpr {
            left: Box::new(TupleValue(0)),
            op: Operator::Eq,
            right: Box::new(Literal(UnsignedLong(2)))
        };

        let plan = Selection {
            expr: filter_expr,
            input: Box::new(csv)
        };

        let s = serde_json::to_string(&plan).unwrap();
        println!("serialized: {}", s);
    }

}

