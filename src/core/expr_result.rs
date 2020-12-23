use std::{collections::HashMap, fmt};

use sqlparser::ast;

use super::{file::*, column::*};

pub enum ExprResult {
    None,
    CompoundSelect(/*table_name*/ String, /*selector*/ Box<dyn Fn(&CoreFile) -> FileColumn>),
    Select(/*selector*/ Box<dyn Fn(&CoreFile) -> FileColumn>),
    Value(ast::Value),
    /* TODO: The Strings should be optional if the comparee is an ast::Value */
    BinaryOp((/*table_name*/ String, /*selector*/ Box<dyn Fn(&CoreFile) -> FileColumn>), /*comparer*/ ast::BinaryOperator, (/*table_name*/ String, /*selector*/ Box<dyn Fn(&CoreFile) -> FileColumn>))
}

impl fmt::Debug for ExprResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExprResult::Select(_) => { write!(f, "ExprResult::Select") }
            ExprResult::CompoundSelect(_, _) => { write!(f, "ExprResult::CompoundSelect") }
            ExprResult::Value(v) => { write!(f, "ExprResult::Value = {:#?}", v) }
            ExprResult::BinaryOp(a, b, c) => { write!(f, "ExprResult::BinaryOp, op = {:#?}", b) }
            _ => { unimplemented!("ExprResult type not handled in debug format") }
        }
    }
}