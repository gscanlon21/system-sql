use std::{cmp::Ordering, error::Error, str::FromStr, ffi::OsString, fmt::{Debug, Display}, fmt, fs::{self, DirEntry, File, FileType, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter};
use sqlparser::{parser::ParserError, ast::Query};
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};

pub trait WithValue<T> {
    fn value() -> T;
}

trait FileColumnTrait {

}

#[derive(Debug, AsStaticStr, EnumIter, PartialEq, PartialOrd)]
pub enum FileColumn {
    Name(Option<String>),
    Path(Option<PathBuf>),
    Type(Option<String>),
    FileExtension(Option<String>),
    Size(Option<u64>),
    AbsolutePath(Option<PathBuf>),
    Created(Option<u32>)
}


impl std::ops::Add for FileColumn {
    type Output = FileColumn;
    fn add(self, rhs: FileColumn) -> Self::Output {
        match (self, rhs) {
            (FileColumn::Size(Some(a)), FileColumn::Size(Some(b))) => { FileColumn::Size(Some(a + b)) }
            _ => { unimplemented!() }
        }
    }
}

impl fmt::Display for FileColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self, f)
    }
}


#[derive(Debug, AsStaticStr, EnumIter)]
pub enum Column {
    Name,
    Path,
    Type,
    FileExtension,
    Size,
    AbsolutePath,
    Created
}


impl Column {
    pub fn iterator() -> Iter<'static, Column> {
        [Column::Name, Column::Path, Column::Type, Column::FileExtension, Column::Size, Column::AbsolutePath, Column::Created].iter()
    }
}

impl FromStr for Column {
    type Err = ();

    fn from_str(input: &str) -> Result<Column, Self::Err> {
        let str: &str = &input.to_ascii_lowercase();
        match str {
            "name"  => Ok(Column::Name),
            "path"  => Ok(Column::Path),
            "type"  => Ok(Column::Type),
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

