
use sqlparser::ast::*;
use std::{io::Error, fs::File, fs};
use std::fs::DirEntry;
use sqlparser::ast::Expr::Value;
use std::env;
use std::vec;
use std::path::Path;
use crate::core::*;
use serde_json::to_string;

pub fn write_csv(files: Vec<CoreFile>, file_path: &String) {
    let mut wtr = csv::Writer::from_path(file_path).unwrap();
                    
    wtr.write_record(&["Path", "Size", "Last Modified"]).unwrap();

    for file in files {
        let metadata = file.metadata().unwrap();
        let size = metadata.len();
        let modified = metadata.modified().unwrap().elapsed().unwrap().as_millis();
        wtr.write_record(&[file.path.unwrap_or_default().to_str().unwrap_or_default(), size.to_string().as_str(), &modified.to_string()[..]]).unwrap();
    }

    wtr.flush().unwrap();
}

pub fn write_json(files: Vec<CoreFile>, file_path: &String) {
    for file in files {
        println!("JSON:\n{}", to_string(&file).unwrap());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_csv() {
        let output_path = "./temp.csv";
        File::create(&output_path).expect("Failure writing temp file");
        let files = fs::read_dir(env::current_dir().expect("Failure reading current directory"))
                                .expect("Failure reading current directory")
                                .into_iter().map(|f|
                                    CoreFile::from(f.unwrap())
                                ).collect();
        
        super::write_csv(files, &output_path.to_string());

        let csv = fs::metadata(output_path).expect("Output file should exist");
        assert_eq!(csv.len() > 8, true); 
    }
}


