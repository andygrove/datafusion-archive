//use std::error::Error;

#[derive(Debug)]
enum DataType {
    UnsignedLong,
    String
}

#[derive(Debug)]
struct ColumnType {
    name: String,
    data_type: DataType,
    nullable: bool
}

#[derive(Debug)]
struct TupleType {
    columns: Vec<ColumnType>
}

enum Value {
    UnsignedLong(u64),
    String(String)
}

/// The tuple trait provides type-safe access to individual values within the tuple
trait Tuple {
    fn get_unsigned_long(index: u32) -> u64;
    fn get_string(index: u32) -> String;
}

enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

enum Expr {
    /// index into a value within the tuple
    TupleValue(u32),
    /// literal value
    Literal(Value),
    /// binary expression e.g. "age > 21"
    BinaryExpr { left: Box<Expr>, op: Operator, right: Box<Expr> },
}

//enum PlanNode {
//    TableScan,
//    IndexScan,
//    Filter,
//    Sort,
//    Project,
//    Join
//}

fn main() {

    let tt = TupleType {
        columns: vec![
            ColumnType { name: String::from("id"), data_type: DataType::UnsignedLong, nullable: false },
            ColumnType { name: String::from("name"), data_type: DataType::String, nullable: false }
        ]
    };

    println!("Tuple type: {:?}", tt);

}
