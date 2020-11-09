use std::{str::FromStr, fs};

use sqlparser::ast::{Assignment, BinaryOperator, Expr, Ident, ObjectName, TableFactor, Value};

use super::{column::Column, error::CoreError, expr_result::ExprResult, file::CoreFile};


pub struct Update {
    table_name: ObjectName,
    assignments: Vec<Assignment>,
    selections: Option<Expr>,

    files: Vec<CoreFile>
}

impl Update {
    pub fn visit(&self) -> Vec<CoreFile> {

        self.consume_expr(self.selection.unwrap(), None, self.table_name)
    }

    fn query_files(&mut self) -> Result<(), CoreError> {
        for selection in &self.selections {
            match selection {
                Expr::Identifier(ident) => { 
                    //self.files: Vec<CoreFile> = Vec::new();
                    let paths = fs::read_dir(&self.table_name.0[0].value)?;
                    for path in paths {
                        if let Ok(path) = path {
                            self.files.push(CoreFile::from(path));
                        }                        
                    }
                }
                Expr::CompoundIdentifier(mut idents) => { 
                    let column = idents.pop().unwrap();
                    let table = idents.pop().unwrap();
                    
                    //Ok(ExprResult::Select())

                    Err(CoreError::from(()))
                }
                Expr::BinaryOp { left, op, right } =>{ 
                    let left = match consume_expr(*left, core_file) {
                        Ok(expr) => {
                            match expr {
                                ExprResult::Select(select) => {
                                    Some(select(core_file.unwrap()))
                                }
                                ExprResult::Value(literal) => { Some(literal) }
                                _ => { None }
                            }
                        }
                        Err(_) => { None }
                    }.unwrap();

                    let right = match consume_expr(*right, core_file) {
                        Ok(expr) => {
                            match expr {
                                ExprResult::Select(select) => {
                                    Some(select(core_file.unwrap()))
                                }
                                ExprResult::Value(literal) => { Some(literal) }
                                _ => { None }
                            }
                        }
                        Err(_) => { None }
                    }.unwrap();

                    println!("binary_op: {{ left: {:?}, right: {:?} }}", left, right);

                    Ok(ExprResult::Value(consume_op(left, op, right)))
                }
                Expr::Value(value) => { Ok(ExprResult::Value(value)) }
                _ => { unimplemented!( )}
            }
        }

        Ok(())
    }










        
    fn consume_expr(&self, expr: Expr) -> Result<ExprResult, CoreError> {
        match expr {
            Expr::Identifier(ident) => { consume_expr_ident(ident, core_file) }
            Expr::CompoundIdentifier(mut idents) => { 
                let column = idents.pop().unwrap();
                let table = idents.pop().unwrap();
                
                //Ok(ExprResult::Select())

                Err(CoreError::from(()))
            }
            Expr::BinaryOp { left, op, right } =>{ 
                let left = match consume_expr(*left, core_file) {
                    Ok(expr) => {
                        match expr {
                            ExprResult::Select(select) => {
                                Some(select(core_file.unwrap()))
                            }
                            ExprResult::Value(literal) => { Some(literal) }
                            _ => { None }
                        }
                    }
                    Err(_) => { None }
                }.unwrap();

                let right = match consume_expr(*right, core_file) {
                    Ok(expr) => {
                        match expr {
                            ExprResult::Select(select) => {
                                Some(select(core_file.unwrap()))
                            }
                            ExprResult::Value(literal) => { Some(literal) }
                            _ => { None }
                        }
                    }
                    Err(_) => { None }
                }.unwrap();

                println!("binary_op: {{ left: {:?}, right: {:?} }}", left, right);

                Ok(ExprResult::Value(consume_op(left, op, right)))
            }
            Expr::Value(value) => { Ok(ExprResult::Value(value)) }
            _ => { unimplemented!( )}
        }
    }

    fn consume_expr_ident(&self, ident: Ident, core_file: Option<&CoreFile>) -> Result<ExprResult, CoreError> {    
        match Column::from_str(ident.value.as_str()) {
            Ok(column) => { Ok(ExprResult::Select(Box::new(move |c| Value::SingleQuotedString(c.value(&column))))) }
            Err(e) => { Err(CoreError::from(e)) }
        }
    }

    fn consume_op(&self, left: Value, op: BinaryOperator, right: Value) -> Value {
        println!("op: {{ left: {:?}, right: {:?} }}", left, right);
        match op {
            BinaryOperator::Plus => { 
                match (left, right) {
                    (Value::Number(a), Value::Number(b)) => { Value::Number((a.parse::<i32>().unwrap() + b.parse::<i32>().unwrap()).to_string()) }
                    (Value::Number(a), Value::Null) => { Value::Number(a) }
                    (Value::SingleQuotedString(mut a), Value::SingleQuotedString(b)) => { a.push_str(b.as_str()); Value::SingleQuotedString(a) }
                    (Value::SingleQuotedString(a), Value::Null) => { Value::SingleQuotedString(a) }
                    (Value::Boolean(a), Value::Null) => { Value::Boolean(a) }
                    (Value::Null, Value::Number(b)) => { Value::Number(b) }
                    (Value::Null, Value::SingleQuotedString(b)) => { Value::SingleQuotedString(b) }
                    (Value::Null, Value::Boolean(b)) => { Value::Boolean(b) }
                    (Value::Null, Value::Null) => { Value::Null }
                    _ => { panic!() }
                }
            }
            BinaryOperator::Eq => { Value::Boolean(left == right) }
            _ => { unimplemented!() }
        }
    }

    fn consume_relation(&self, relation: TableFactor) -> Result<Vec<CoreFile>, CoreError> {
        let mut files: Vec<CoreFile> = Vec::new();
        match relation {
            TableFactor::Table { name, alias, args, with_hints } => {
                if let Some(table_name) = name.0.first() {
                    files = consume_table_name(table_name.value.as_str())?;
                }
            }
            TableFactor::Derived { lateral, subquery, alias } => {}
            TableFactor::NestedJoin(_) => {}
        }

        return Ok(files);
    }

    fn consume_table_name(&self, table_name: &str) -> Result<Vec<CoreFile>, CoreError> {
        let mut files: Vec<CoreFile> = Vec::new();
        let paths = fs::read_dir(table_name)?;
        for path in paths {
            if let Ok(path) = path {
                files.push(CoreFile::from(path));
            }                        
        }

        Ok(files)
    }

}

