#![allow(unused_imports, dead_code, unused_variables)]

mod core;
mod query;
mod display;

extern crate csv;

use std::env;
use crate::core::*;
use crate::query::*;
use crate::display::*;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 1 {
        println!("Invalid arguments");
        std::process::exit(1);
    }

    let sql = &args[1];
    std::process::exit(match parse_sql(&sql) {
        Ok(_) => {
            println!("Success!");
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
        let sql = "SELECT * FROM [./]";
        super::parse_sql(sql).unwrap();

        let sql = "SELECT * FROM [./../]";
        super::parse_sql(sql).unwrap();

        let sql = "SELECT * FROM [.]";
        super::parse_sql(sql).unwrap();

        let sql = "SELECT * FROM [/]";
        super::parse_sql(sql).unwrap();
    }
}
