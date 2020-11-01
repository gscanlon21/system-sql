use std::{cmp::Ordering, error::Error, str::FromStr, ffi::OsString, fmt::{Debug, Display}, fmt, fs::{self, DirEntry, File, FileType, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter};
use sqlparser::{parser::ParserError, ast::Query};
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};


#[derive(Debug, AsStaticStr, EnumIter)]
pub enum Column {
    Name,
    Path,
    FileType,
    FileExtension,
    Size,
    AbsolutePath,
    Created
}


impl Column {
    pub fn iterator() -> Iter<'static, Column> {
        [Column::Name, Column::Path, Column::FileType, Column::FileExtension, Column::Size, Column::AbsolutePath, Column::Created].iter()
    }
}

impl FromStr for Column {
    type Err = ();

    fn from_str(input: &str) -> Result<Column, Self::Err> {
        let str: &str = &input.to_ascii_lowercase();
        match str {
            "name"  => Ok(Column::Name),
            "path"  => Ok(Column::Path),
            "filetype" | "file_type"  => Ok(Column::FileType),
            "file_extension" | "fileextension" => Ok(Column::FileExtension),
            "size" => Ok(Column::Size),
            "absolutepath" | "absolute_path" => Ok(Column::AbsolutePath),
            _ => Err(()),
        }
    }
}


impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

