use std::{collections::HashMap, fmt};

use sqlparser::ast;

use super::file::CoreFile;


pub enum ExprResult {
    None,
    CompoundSelect(String, Box<dyn Fn(&CoreFile) -> ast::Value>),
    Select(Box<dyn Fn(&CoreFile) -> ast::Value>),
    Assignment(CoreFile),
    Value(ast::Value),
    Filter(bool),
    Filter2(Box<dyn Fn(CoreFile, CoreFile) -> ast::Value>),
    Expr(ast::Expr),
    BinaryOp((String, Box<dyn Fn(&CoreFile) -> ast::Value>), ast::BinaryOperator, (String, Box<dyn Fn(&CoreFile) -> ast::Value>))
}

impl fmt::Debug for ExprResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExprResult::Select(_) => { write!(f, "SELECT") }
            ExprResult::Assignment(a) => { write!(f, "{:#?}", a) }
            ExprResult::Value(r) => { write!(f, "{:#?}", r) }
            ExprResult::Expr(r) => { write!(f, "{:#?}", r) }
            ExprResult::Filter(b) => { write!(f, "{:#?}", b) }
            ExprResult::BinaryOp(a, b, c) => { write!(f, "op: {:#?}", b) }
            _ => { Ok(()) }
        }
    }
}