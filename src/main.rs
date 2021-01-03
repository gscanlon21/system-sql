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

    let program_name = &args[0]; // The program name
    let sql = &args[1]; // The SQL string

    // The dialect used to parse the SQL string
    let dialect = match args.get(2) {
        Some(s) => { CoreDialect::from_str(s) }
        None => { Err(()) }
    }.unwrap_or(/* Supports [bracketed] table names which makes it easier to read from directories: "SELECT * FROM [./]" */
        CoreDialect { dialect: Box::new(sqlparser::dialect::MsSqlDialect {}) 
    });

    std::process::exit(match parse_sql(&sql, dialect) {
        Ok(_) => {
            0
        }
        Err(error) => {
            println!("An unexpected error occurred while running {}. The program failed with {}", program_name, error);
            1
        }
    });
}
