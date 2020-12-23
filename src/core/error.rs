use std::{cmp::Ordering, error::Error, str::FromStr, ffi::OsString, fmt::{Debug, Display}, fmt, fs::{self, DirEntry, File, FileType, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter};
use sqlparser::parser;
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};

#[derive(Debug, Clone, PartialEq)]
pub enum CoreError {
    IOError(String),
    ParserError(parser::ParserError),
    CsvError(String),
    GeneralError(String),
    UnknownError(String, u32, u32)
}

impl Error for CoreError {}

impl From<()> for CoreError {
    fn from(e: ()) -> Self {
        CoreError::GeneralError("Unknown Error".to_string())
    }
}

impl From<csv::Error> for CoreError {
    fn from(e: csv::Error) -> Self {
        CoreError::IOError(format!("{}", e))
    }
}


impl From<io::Error> for CoreError {
    fn from(e: io::Error) -> Self {
        CoreError::IOError(format!("{}", e))
    }
}

impl From<parser::ParserError> for CoreError {
    fn from(e: parser::ParserError) -> Self {
        CoreError::ParserError(e)
    }
}

impl From<&str> for CoreError {
    fn from(e: &str) -> Self {
        CoreError::GeneralError(format!("{}", e))
    }
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
            "error: {}",
            match self {
                CoreError::IOError(e) => e.to_string(),
                CoreError::ParserError(e) => e.to_string(),
                CoreError::CsvError(e) => e.to_string(),
                CoreError::GeneralError(e) => e.to_string(),
                CoreError::UnknownError(filename, line, col) => format!("file {}, line {}, col: {}", filename, line, col),
            }
        )
    }
}

