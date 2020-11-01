use std::{cmp::Ordering, error::Error, str::FromStr, ffi::OsString, fmt::{Debug, Display}, fmt, fs::{self, DirEntry, File, FileType, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter};
use sqlparser::{parser::ParserError, ast::Query};
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};

#[derive(Debug, Clone, PartialEq)]
pub enum CoreError {
    IOError(String),
    ParserError(String),
    GeneralError(String),
    UnknownError
}

impl From<()> for CoreError {
    fn from(e: ()) -> Self {
        CoreError::UnknownError
    }
}

impl From<io::Error> for CoreError {
    fn from(e: io::Error) -> Self {
        CoreError::IOError(format!("{}", e))
    }
}

impl From<ParserError> for CoreError {
    fn from(e: ParserError) -> Self {
        CoreError::ParserError(format!("{}", e))
    }
}

impl From<&str> for CoreError {
    fn from(e: &str) -> Self {
        CoreError::GeneralError(format!("{}", e))
    }
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Error: {}",
            match self {
                CoreError::IOError(s) => s,
                CoreError::ParserError(s) => s,
                CoreError::GeneralError(s) => s,
                CoreError::UnknownError => "Unknown, please change to known error",
            }
        )
    }
}

