use std::{collections::HashMap, fmt};

use sqlparser::ast;

use super::{file::*, column::*};


pub enum ExprResult {
    None,
    CompoundSelect(String, Box<dyn Fn(&CoreFile) -> FileColumn>),
    Select(Box<dyn Fn(&CoreFile) -> FileColumn>),
    Value(ast::Value),
    BinaryOp((/* should be optional if the comparee is an ast::Value --> */String, Box<dyn Fn(&CoreFile) -> FileColumn>), ast::BinaryOperator, (String, Box<dyn Fn(&CoreFile) -> FileColumn>))
}

impl fmt::Debug for ExprResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExprResult::Select(_) => { write!(f, "SELECT") }
            ExprResult::CompoundSelect(_, _) => { write!(f, "Compound SELECT") }
            ExprResult::Value(r) => { write!(f, "{:#?}", r) }
            ExprResult::BinaryOp(a, b, c) => { write!(f, "op: {:#?}", b) }
            _ => { Ok(()) }
        }
    }
}