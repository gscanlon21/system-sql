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

// impl Update {
//     pub fn new(table_name: ObjectName, assignments: Vec<Assignment>, selection: Option<Expr>) -> Self {
//         Update {
//             table_name,
//             assignments,
//             selection,
//             files: Update::consume_table_name(table_name).unwrap()
//         }
//     }

//     fn consume_table_name(table_name: ObjectName) -> Result<Vec<CoreFile>, CoreError> {
//         let mut files: Vec<CoreFile> = Vec::new();
//         let paths = fs::read_dir(table_name.0[0].value)?;
//         for path in paths {
//             if let Ok(path) = path {
//                 files.push(CoreFile::from(path));
//             }                        
//         }

//         Ok(files)
//     }

//     fn visit(&self) -> Self {
//         if let Some(selection) = self.selection {
//             self.consume_expr(selection);
//         }
//         *self
//     }
        
//     fn consume_expr(&mut self, expr: Expr) -> Result<ExprResult, CoreError> {
//         match expr {
//             Expr::Identifier(ident) => { self.consume_expr_ident(ident) }
//             Expr::CompoundIdentifier(mut idents) => { 
//                 let column = idents.pop().unwrap();
//                 let table = idents.pop().unwrap();
                
//                 self.consume_expr(Expr::Identifier(column))
//             }
//             Expr::BinaryOp { left, op, right } => { 
//                 let left_result = self.consume_expr(*left);
//                 let right_result = self.consume_expr(*right);

//                 for core_file in &self.files {
//                     let left = match left_result {
//                         Ok(expr) => {
//                             match expr {
//                                 ExprResult::Select(select) => {
//                                     Some(select(core_file))
//                                 }
//                                 ExprResult::Value(literal) => { Some(literal) }
//                                 _ => { None }
//                             }
//                         }
//                         Err(_) => { None }
//                     }.unwrap();
        
//                     let right = match right_result {
//                         Ok(expr) => {
//                             match expr {
//                                 ExprResult::Select(select) => {
//                                     Some(select(core_file))
//                                 }
//                                 ExprResult::Value(literal) => { Some(literal) }
//                                 _ => { None }
//                             }
//                         }
//                         Err(_) => { None }
//                     }.unwrap();    

//                     println!("binary_op: {{ left: {:?}, right: {:?} }}", left, right);

//                     match self.consume_op(left, op, right) {
//                         Value::Boolean(b) => { 
//                             if b == false {
//                                 self.files.remove(self.files.iter().position(|f| f == core_file).unwrap());
//                             }
//                          }
//                         _ => { panic!() }
//                     }
//                 }

//                 Ok(ExprResult::Value(Value::Boolean(true)))
//             }
//             Expr::Value(value) => { Ok(ExprResult::Value(value)) }
//             _ => { unimplemented!( )}
//         }
//     }
    
//     fn consume_expr_ident(&self, ident: Ident) -> Result<ExprResult, CoreError> {    
//         match Column::from_str(ident.value.as_str()) {
//             Ok(column) => { Ok(ExprResult::Select(Box::new(move |c| Value::SingleQuotedString(c.value(&column))))) }
//             Err(e) => { Err(CoreError::from(e)) }
//         }
//     }
    
//     fn consume_op(&self, left: Value, op: BinaryOperator, right: Value) -> Value {
//         println!("op: {{ left: {:?}, right: {:?} }}", left, right);
//         match op {
//             BinaryOperator::Plus => { 
//                 match (left, right) {
//                     (Value::Number(a), Value::Number(b)) => { Value::Number((a.parse::<i32>().unwrap() + b.parse::<i32>().unwrap()).to_string()) }
//                     (Value::Number(a), Value::Null) => { Value::Number(a) }
//                     (Value::SingleQuotedString(mut a), Value::SingleQuotedString(b)) => { a.push_str(b.as_str()); Value::SingleQuotedString(a) }
//                     (Value::SingleQuotedString(a), Value::Null) => { Value::SingleQuotedString(a) }
//                     (Value::Boolean(a), Value::Null) => { Value::Boolean(a) }
//                     (Value::Null, Value::Number(b)) => { Value::Number(b) }
//                     (Value::Null, Value::SingleQuotedString(b)) => { Value::SingleQuotedString(b) }
//                     (Value::Null, Value::Boolean(b)) => { Value::Boolean(b) }
//                     (Value::Null, Value::Null) => { Value::Null }
//                     _ => { panic!() }
//                 }
//              }
//             BinaryOperator::Eq => { Value::Boolean(left == right) }
//             _ => { unimplemented!() }
//         }
//     }
// }

