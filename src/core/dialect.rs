use std::{cmp::Ordering, error::Error, str::FromStr, ffi::OsString, fmt::{Debug, Display}, fmt, fs::{self, DirEntry, File, FileType, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter};
use sqlparser::{parser::ParserError, ast::Query};
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};


pub struct CoreDialect {
    pub dialect: Box<dyn sqlparser::dialect::Dialect>
}

impl FromStr for CoreDialect {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &s.to_ascii_lowercase()[..] {
            "mssql" => { Ok(CoreDialect { dialect: Box::new(sqlparser::dialect::MsSqlDialect {}) }) }
            "generic" => { Ok(CoreDialect { dialect: Box::new(sqlparser::dialect::GenericDialect) }) }
            _ => { Err(()) }
        }
    }
}