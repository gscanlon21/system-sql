
use sqlparser::ast::*;
use std::{fs::File, io::Write, fs, io::Error};
use std::fs::DirEntry;
use sqlparser::ast::Expr::Value;
use std::env;
use std::vec;
use std::path::Path;
use crate::core::{column::*, file::*};
use serde_json::{json, to_string};

pub fn write_csv(columns: Vec<FileColumn>, files: Vec<Vec<FileColumn>>, file_path: &String) {
    //println!("csv files: {:#?}", files.clone());

    let mut wtr = csv::Writer::from_path(file_path).unwrap();
                    
    // columns.iter().map(|c| c.to_string())
    wtr.write_record(&["1","2", "3","4"]).unwrap();

    for rows in files {
        let mut cells: Vec<String> = Vec::new();
        for file in rows {
            for column in &columns {
                cells.push(file.to_string());
            }
        }
        
        println!("cells: {:#?}", cells.clone());
        wtr.write_record(cells).unwrap();
    }

    wtr.flush().unwrap();
}

pub fn write_json(columns: Vec<FileColumn>, files: Vec<Vec<FileColumn>>, file_path: &String) {
    let json = json!(files);
    //println!("{:#?}", json);
    print!("{}", json);
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


