
use sqlparser::ast::*;
use std::{fs::File, io::Write, fs, io::Error};
use std::fs::DirEntry;
use sqlparser::ast::Expr::Value;
use std::env;
use std::vec;
use std::path::Path;
use crate::core::{column::*, error::CoreError, file::*};
use serde_json::{json, to_string};

pub fn write_csv(columns: Vec<FileColumn>, files: Vec<Vec<FileColumn>>, file_path: &String) -> Result<(), CoreError> {
    println!("{:#?}", columns);
    
    let mut wtr = csv::Writer::from_path(file_path)?;
                    
    wtr.write_record(columns.iter().map(|c| c.to_string()))?;

    for rows in files {
        let mut cells: Vec<String> = Vec::new();
        for file in rows {
            cells.push(file.to_string());
        }

        while cells.len() < columns.len() {
            cells.push(String::from(""));
        }
        
        println!("cells: {:#?}", cells.clone());
        wtr.write_record(cells)?;
    }

    wtr.flush()?;

    Ok(())
}

pub fn write_json(columns: Vec<FileColumn>, files: Vec<Vec<FileColumn>>, file_path: &String)  -> Result<(), CoreError> {
    let json = json!(files);
    print!("{}", json);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_csv() {
        // let output_path = "./temp.csv";
        // File::create(&output_path).expect("Failure writing temp file");
        // let files = fs::read_dir(env::current_dir().expect("Failure reading current directory"))
        //                         .expect("Failure reading current directory")
        //                         .into_iter().map(|f|
        //                             CoreFile::from(f.unwrap())
        //                         ).collect();
        
        // super::write_csv(files, &output_path.to_string());

        // let csv = fs::metadata(output_path).expect("Output file should exist");
        // assert_eq!(csv.len() > 8, true); 
    }
}


