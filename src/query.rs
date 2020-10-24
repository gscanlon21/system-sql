
use sqlparser::dialect::MsSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;
use sqlparser::ast::SetExpr;
use sqlparser::ast::TableFactor;
use sqlparser::ast::Select;
use sqlparser::ast::Query;
use sqlparser::ast::Top;
use sqlparser::ast::Ident;
use std::fs::{self, DirEntry};
use std::env;
use std::path::Path;
use crate::core::*;
use crate::select::*;

pub fn consume_query(query: Query) -> Result<Vec<DirEntry>, String> {
    match query.body {
        SetExpr::Select(select) => { 
            let selected = consume_select(*select);
            match &selected {
                Ok(files) => {
                    println!("\nOk! Here are the results:");
                    for file in files {
                        println!("File: {:#?}", file);
                    }
                }
                Err(error) => {
                    println!("Error: {}", error)
                }
            }
            return selected;
        }
        SetExpr::Query(_) => { Err(String::from("Unimplemented")) }
        SetExpr::SetOperation { op, all, left, right } => { Err(String::from("Unimplemented")) }
        SetExpr::Values(_) => { Err(String::from("Unimplemented")) }
    }
}