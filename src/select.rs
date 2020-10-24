
use sqlparser::dialect::MsSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;
use sqlparser::ast::SetExpr;
use sqlparser::ast::TableFactor;
use sqlparser::ast::Select;
use sqlparser::ast::Query;
use sqlparser::ast::JoinOperator;
use sqlparser::ast::Top;
use sqlparser::ast::Ident;
use std::{io::Error, fs};
use std::fs::DirEntry;
use sqlparser::ast::Expr::Value;
use std::env;
use std::vec;
use std::path::Path;
use crate::core::*;

fn consume_relation(relation: TableFactor) -> Result<Vec<DirEntry>, String> {
    let mut files: Vec<DirEntry> = Vec::new();
    match relation {
        TableFactor::Table { name, alias, args, with_hints } => {
            if let Some(table_name) = name.0.first() {
                match fs::read_dir(table_name.value.clone()) {
                    Ok(paths) => {
                        for path in paths {
                            if let Ok(path) = path {
                                // let metadata = fs::metadata(&path)?;
                                files.push(path);
                            }                        
                        }
                    }
                    Err(error) => {
                        return Err(error.to_string())
                    }
                }
            }
        }
        TableFactor::Derived { lateral, subquery, alias } => {}
        TableFactor::NestedJoin(_) => {}
    }

    return Ok(files);
}


pub fn consume_select(select: Select) -> Result<Vec<DirEntry>, String> {
    for tableWithJoin in select.from {
        let mut files = consume_relation(tableWithJoin.relation).unwrap();

        for join in tableWithJoin.joins {
            match consume_relation(join.relation) {
                Ok(joinFiles) => {
                    match join.join_operator {
                        JoinOperator::Inner(constraint) => {
                            match constraint {
                                sqlparser::ast::JoinConstraint::On(expr) => {}
                                sqlparser::ast::JoinConstraint::Using(_) => {}
                                sqlparser::ast::JoinConstraint::Natural => {}
                            }
                        }
                        JoinOperator::LeftOuter(_) => {}
                        JoinOperator::RightOuter(_) => {}
                        JoinOperator::FullOuter(_) => {}
                        JoinOperator::CrossJoin => {}
                        JoinOperator::CrossApply => {}
                        JoinOperator::OuterApply => {}
                    }
                }
                Err(error) => {}
            }
        }

        if select.distinct {
            files.sort_by(|a, b| a.path().cmp(&b.path()));
            files.dedup_by(|a, b| a.path().eq(&b.path()))
        }

        if let Some(ref top) = select.top  {
            if let Value(ref value) = top.quantity.as_ref().unwrap() {
                let quantity: Option<usize> = match value {
                    sqlparser::ast::Value::Number(str) => { str.parse().ok() }
                    sqlparser::ast::Value::SingleQuotedString(str) => { str.parse().ok() }
                    sqlparser::ast::Value::NationalStringLiteral(str) => { str.parse().ok() }
                    sqlparser::ast::Value::HexStringLiteral(str) => { str.parse().ok() }
                    sqlparser::ast::Value::Boolean(b) => { Some(*b as usize) }
                    sqlparser::ast::Value::Interval { value, leading_field, leading_precision, last_field, fractional_seconds_precision } => { None }
                    sqlparser::ast::Value::Null => { None }
                };

                files = if let Some(quantity) = quantity {
                    files.into_iter().take(quantity).collect()
                } else {
                    files
                }
            }
        }

        return Ok(files)
    }

    let test = vec![0; 5];
    let temp = &test[0..3];

    

    

    for group in select.group_by {

    }

    if let Some(seelction) = select.selection {

    }

    for projection in select.projection {

    }

    if let Some(having) = select.having {

    }

    return Err(String::from("There's nothing here!"));
}

#[cfg(test)]
mod tests {
    use super::*;

    static SQL: &'static str = "SELECT TOP 1 Name FROM ls";

    #[test]
    fn top_1() {
        let result = super::generate(SQL.to_string());
        assert_eq!(result.iter().count(), 1); 
    }
    
}


