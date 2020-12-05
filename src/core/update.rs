use std::{str::FromStr, fs};

use sqlparser::ast::{Assignment, BinaryOperator, Expr, Ident, ObjectName, TableFactor, Value};

use super::{file::*, error::CoreError, expr_result::ExprResult};


pub struct Update {
    table_name: ObjectName,
    assignments: Vec<Assignment>,
    selection: Option<Expr>,

    files: Vec<CoreFile>
}

impl Iterator for Update {
    type Item = CoreFile;
    fn next(&mut self) -> Option<Self::Item> {
        self.files.pop()
    }
}
