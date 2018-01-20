
/// Value holder for all supported data types
#[derive(Debug,Clone,PartialEq,PartialOrd,Serialize,Deserialize)]
pub enum Value {
    UnsignedLong(u64),
    String(String),
    Boolean(bool),
    Double(f64)
}

#[derive(Debug,Serialize, Deserialize)]
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
        let filter_expr = BinaryExpr {
            left: Box::new(TupleValue(0)),
            op: Operator::Eq,
            right: Box::new(Literal(UnsignedLong(2)))
        };

        let plan = Selection {
            expr: filter_expr,
            input: Box::new(
                TableScan {
                    schema: "foo".to_string(),
                    table: "bar".to_string()
                })
        };

        let s = serde_json::to_string(&plan).unwrap();
        println!("serialized: {}", s);
    }

}

