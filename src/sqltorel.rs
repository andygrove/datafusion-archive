use std::string::String;

use super::sql::*;
use super::rel::*;
use super::schema::*;

pub struct SqlToRel {
    default_schema: Option<String>
}

impl SqlToRel {

    pub fn sql_to_rel(&self, sql: &ASTNode, tt: &TupleType) -> Box<Rel> {
        match sql {
            &ASTNode::SQLSelect { ref projection, ref relation, .. } => {

                let expr : Vec<Rex> = projection.iter()
                    .map(|e| self.sql_to_rex(&e, tt))
                    .collect();

                let input = match relation {
                    &Some(ref r) => Some(self.sql_to_rel(r, tt)),
                    &None => None
                };

                Box::new(Rel::Projection {
                    expr: expr,
                    input: input
                })
            },

            _ => panic!("not implemented")
        }
    }

    pub fn sql_to_rex(&self, sql: &ASTNode, tt: &TupleType) -> Rex {
        match sql {
            &ASTNode::SQLIdentifier { ref id, .. } => {
                match tt.columns.iter().position(|c| c.name.eq(id) ) {
                    Some(index) => Rex::TupleValue(index),
                    None => panic!("Invalid identifier")
                }
            },
            _ => panic!("not implemented")
        }
    }

}
