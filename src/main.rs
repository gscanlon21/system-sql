#![allow(unused_imports, dead_code, unused_variables)]

mod core;
mod query;
mod display;
mod enumerable;

#[macro_use]
extern crate strum_macros;

use std::{env, str::FromStr, collections::HashMap};
use crate::core::{dialect::CoreDialect};
use crate::query::*;
use crate::display::*;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() <= 1 {
        println!("Invalid arguments");
        std::process::exit(1);
    }

    let _ = &args[0]; // Program Name
    let sql = &args[1];
    let dialect = match args.get(2) {
        Some(s) => { CoreDialect::from_str(s) }
        None => { Err(()) }
    }.unwrap_or(CoreDialect { dialect: Box::new(sqlparser::dialect::MsSqlDialect {}) });

    std::process::exit(match parse_sql(&sql, dialect) {
        Ok(_) => {
            //println!("Success!");
            0
        }
        Err(error) => {
            println!("Failure: {}", error);
            1
        }
    });
}

#[cfg(test)]
mod tests {
    #[test]
    fn parses_file_path_table_names() {
        // let sql = "SELECT * FROM [./]";
        // super::parse_sql(sql).unwrap();

        // let sql = "SELECT * FROM [./../]";
        // super::parse_sql(sql).unwrap();

        // let sql = "SELECT * FROM [.]";
        // super::parse_sql(sql).unwrap();

        // let sql = "SELECT * FROM [/]";
        // super::parse_sql(sql).unwrap();
    }
}
