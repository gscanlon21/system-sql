use std::fmt;

use sqlparser::ast;

use super::file::CoreFile;


pub enum ExprResult {
    Select(Box<dyn Fn(&CoreFile) -> ast::Value>),
    Assignment(CoreFile),
    Value(ast::Value),
    Filter(bool),
    Filter2(Box<dyn Fn(&CoreFile) -> ast::Value>),
    Expr(ast::Expr)
}

impl fmt::Debug for ExprResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExprResult::Select(_) => { write!(f, "SELECT") }
            ExprResult::Assignment(a) => { write!(f, "{:#?}", a) }
            ExprResult::Value(r) => { write!(f, "{:#?}", r) }
            ExprResult::Expr(r) => { write!(f, "{:#?}", r) }
            ExprResult::Filter(b) => { write!(f, "{:#?}", b) }
        }
    }
}