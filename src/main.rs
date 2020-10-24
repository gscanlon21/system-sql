mod core;
mod select;
mod query;

extern crate csv;

use sqlparser::dialect::MsSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Statement;
use sqlparser::ast::SetExpr;
use sqlparser::ast::TableFactor;
use sqlparser::ast::Select;
use sqlparser::ast::Query;
use sqlparser::ast::Top;
use sqlparser::ast::Ident;
use std::fs;
use std::env;
use std::path::Path;
use crate::core::*;
use crate::select::*;
use crate::query::*;

fn main() {
    let args: Vec<String> = env::args().collect();

    let sql = &args[1];

    let dialect = MsSqlDialect {}; // Supports [bracketed] table names which makes it easier to read from directories: "SELECT * FROM [./]"

    let parse_result = Parser::parse_sql(&dialect, sql);

    match parse_result {
        Ok(statements) => {
            println!(
                "Round-trip:\n'{}'",
                statements
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("\n")
            );

            println!("Parse results:\n{:#?}", statements);

            for statement in statements {
                match statement {
                    Statement::Query(query) => {
                        consume_query(*query);
                    }
                    Statement::Insert { table_name, columns, source } => {
                        let files = consume_query(*source);

                        if let Some(table_name) = table_name.0.first() {
                            let file_path = &table_name.value;
                            
                            let mut wtr = csv::Writer::from_path(file_path).unwrap();
                    
                            wtr.write_record(&["Path", "Size", "Last Modified"]);

                            for file in files.unwrap() {
                                let metadata = fs::metadata(file.path()).unwrap();
                                let size = metadata.len();
                                let modified = metadata.modified().unwrap().elapsed().unwrap().as_millis();
                                wtr.write_record(&[file.path().to_str().unwrap(), size.to_string().as_str(), &modified.to_string()[..]]);
                            }

                            wtr.flush();
                        };
                    }
                    Statement::Copy { table_name, columns, values } => {}
                    Statement::Update { table_name, assignments, selection } => {}
                    Statement::Delete { table_name, selection } => {}
                    Statement::CreateView { name, columns, query, materialized, with_options } => {}
                    Statement::CreateTable { name, columns, constraints, with_options, if_not_exists, external, file_format, location, query, without_rowid } => {}
                    Statement::CreateVirtualTable { name, if_not_exists, module_name, module_args } => {}
                    Statement::CreateIndex { name, table_name, columns, unique, if_not_exists } => {}
                    Statement::AlterTable { name, operation } => {}
                    Statement::Drop { object_type, if_exists, names, cascade } => {}
                    Statement::SetVariable { local, variable, value } => {}
                    Statement::ShowVariable { variable } => {}
                    Statement::ShowColumns { extended, full, table_name, filter } => {}
                    Statement::StartTransaction { modes } => {}
                    Statement::SetTransaction { modes } => {}
                    Statement::Commit { chain } => {}
                    Statement::Rollback { chain } => {}
                    Statement::CreateSchema { schema_name } => {}
                    Statement::Assert { condition, message } => {}
                }
            }

            std::process::exit(0);
        }
        Err(e) => {
            println!("Error during parsing: {:?}", e);
            std::process::exit(1);
        }
    }
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




