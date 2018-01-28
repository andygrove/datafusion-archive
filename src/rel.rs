// Copyright 2018 Grove Enterprises LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

impl TupleType {

    /// create an empty tuple
    pub fn empty() -> Self { TupleType { columns: vec![] } }

    /// look up a column by name and return a reference to the column along with it's index
    pub fn column(&self, name: &str) -> Option<(usize, &ColumnMeta)> {
        self.columns.iter()
            .enumerate()
            .find(|&(_,c)| c.name == name)
    }

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

impl Tuple {
    pub fn new(v: Vec<Value>) -> Self {
        Tuple { values: v }
    }
}

/// Value holder for all supported data types
#[derive(Debug,Clone,PartialEq,PartialOrd,Serialize,Deserialize)]
pub enum Value {
    UnsignedLong(u64),
    String(String),
    Boolean(bool),
    Double(f64)
}

impl Value {

    fn to_string(&self) -> String {
        unimplemented!()
    }

}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// Relation Expression
#[derive(Debug,Clone,Serialize, Deserialize)]
pub enum Rex {
    /// index into a value within the tuple
    TupleValue(usize),
    /// literal value
    Literal(Value),
    /// binary expression e.g. "age > 21"
    BinaryExpr { left: Box<Rex>, op: Operator, right: Box<Rex> },
}

impl Rex {

    pub fn eq(&self, other: &Rex) -> Rex {
        Rex::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::Eq,
            right: Box::new(other.clone())
        }
    }


}

/// Relations
#[derive(Debug,Clone,Serialize, Deserialize)]
pub enum Rel {
    Projection { expr: Vec<Rex>, input: Box<Rel>, schema: TupleType },
    Selection { expr: Rex, input: Box<Rel>, schema: TupleType },
    TableScan { schema_name: String, table_name: String, schema: TupleType },
    CsvFile { filename: String, schema: TupleType },
    EmptyRelation
}

impl Rel {

    pub fn schema(&self) -> TupleType {
        match self {
            &Rel::EmptyRelation => TupleType::empty(),
            &Rel::TableScan { ref schema, .. } => schema.clone(),
            &Rel::CsvFile { ref schema, .. } => schema.clone(),
            &Rel::Projection { ref schema, .. } => schema.clone(),
            &Rel::Selection { ref schema, .. } => schema.clone(),
        }
    }
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

        let csv = CsvFile { filename: "test/people.csv".to_string(), schema: tt.clone() };

        let filter_expr = BinaryExpr {
            left: Box::new(TupleValue(0)),
            op: Operator::Eq,
            right: Box::new(Literal(UnsignedLong(2)))
        };

        let plan = Selection {
            expr: filter_expr,
            input: Box::new(csv),
            schema: tt.clone()

        };

        let s = serde_json::to_string(&plan).unwrap();
        println!("serialized: {}", s);
    }

}

