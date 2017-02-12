use std::error::Error;
use std::fs::File;

#[derive(Debug, PartialEq)]
pub enum ASTNode {
    SQLIdentifier{id: String, parts: Vec<String>},
    SQLBinary{left: Box<ASTNode>, op: SQLOperator, right: Box<ASTNode>},
    SQLNested(Box<ASTNode>),
    SQLUnary{operator: SQLOperator, Rex: Box<ASTNode>},
//    SQLLiteral(usize),
//    SQLBoundParam(u32),
//    SQLAlias{Rex: Box<ASTNode>, alias: Box<ASTNode>},
//    SQLOrderBy{Rex: Box<ASTNode>, is_asc: bool},
    SQLSelect{
        projection: Vec<ASTNode>,
        relation: Option<Box<ASTNode>>,
        selection: Option<Box<ASTNode>>,
        order: Option<Box<ASTNode>>,
        limit: Option<Box<ASTNode>>,
//        for_update: bool,
    },
//    SQLInsert {
//        table: Box<ASTNode>,
//        insert_mode: InsertMode,
//        column_list: Box<ASTNode>,
//        values_list: Vec<ASTNode>
//    },
//    SQLUpdate {
//        table: Box<ASTNode>,
//        assignments: Box<ASTNode>,
//        selection: Option<Box<ASTNode>>
//    },
//    SQLDelete {
//        table: Box<ASTNode>,
//        selection: Option<Box<ASTNode>>
//    },
//    SQLUnion{left: Box<ASTNode>, union_type: UnionType, right: Box<ASTNode>},
//    SQLJoin{left: Box<ASTNode>, join_type: JoinType, right: Box<ASTNode>, on_expr: Option<Box<ASTNode>>},
//    SQLFunctionCall{identifier: Box<ASTNode>, args: Vec<ASTNode>},
//
//    // MySQL
//    MySQLCreateDatabase {
//        database: Box<ASTNode>,
//    },
//    MySQLDropDatabase {
//        database: Box<ASTNode>,
//        if_exists: bool,
//    },
//    MySQLDropTable {
//        temporary: bool,
//        if_exists: bool,
//        restrict: bool,
//        cascade: bool,
//        tables: Vec<ASTNode>
//    },
//    MySQLCreateTable{
//        table: Box<ASTNode>,
//        column_list: Vec<ASTNode>,
//        keys: Vec<ASTNode>,
//        table_options: Vec<ASTNode>
//    },
//    MySQLColumnDef{column: Box<ASTNode>, data_type: Box<ASTNode>, qualifiers: Option<Vec<ASTNode>>},
//    MySQLKeyDef(MySQLKeyDef),
//    MySQLColumnQualifier(MySQLColumnQualifier),
//    MySQLDataType(MySQLDataType),
//    MySQLTableOption(MySQLTableOption),
//    MySQLUse(Box<ASTNode>)
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
