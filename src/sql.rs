
#[derive(Debug,Clone,PartialEq)]
pub enum ASTNode {
    SQLIdentifier{id: String, parts: Vec<String>},
    SQLBinaryExpr{left: Box<ASTNode>, op: SQLOperator, right: Box<ASTNode>},
    SQLNested(Box<ASTNode>),
    SQLUnary{operator: SQLOperator, rex: Box<ASTNode>},
    SQLLiteralInt(i64),
    SQLSelect{
        projection: Vec<ASTNode>,
        relation: Option<Box<ASTNode>>,
        selection: Option<Box<ASTNode>>,
        order: Option<Box<ASTNode>>,
        limit: Option<Box<ASTNode>>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum SQLOperator {
    ADD,
    SUB,
    MULT,
    DIV,
    MOD,
    GT,
    LT,
    GTEQ,
    LTEQ,
    EQ,
    NEQ,
    OR,
    AND
}
