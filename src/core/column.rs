use std::{cmp::Ordering, error::Error, ffi::OsString, time::SystemTime, fmt::{Debug, Display}, fmt, fs::{self, DirEntry, File, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter, str::FromStr};
use sqlparser::{parser::ParserError, ast::Query};
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};
use super::{error::CoreError, file::file_type::FileType};

pub trait FileColumnValue {
    fn column(&self, column: &FileColumn) -> FileColumn;
    fn columns(&self) -> Vec<FileColumn>;
}

pub trait FileColumnValues {
    fn name(&self) -> FileColumn;
    fn path(&self) -> FileColumn;
    fn file_type(&self) -> FileColumn;
    fn file_extension(&self) -> FileColumn;
    fn size(&self) -> FileColumn;
    fn absolute_path(&self) -> FileColumn;
    fn created(&self) -> FileColumn;
}

#[derive(Debug, AsStaticStr, EnumIter, PartialEq, PartialOrd, Hash, Eq, Ord, Clone)]
pub enum FileColumn {
    Null,
    Name(Option<OsString>),
    Path(Option<PathBuf>),
    Type(Option<FileType>),
    FileExtension(Option<OsString>),
    Size(Option<u64>),
    AbsolutePath(Option<PathBuf>),
    Created(Option<SystemTime>)
}

impl FileColumn {
    pub fn iterator() -> Iter<'static, FileColumn> {
        [FileColumn::Name(None), FileColumn::Path(None), FileColumn::Type(None), FileColumn::FileExtension(None), FileColumn::Size(None), FileColumn::AbsolutePath(None), FileColumn::Created(None)].iter()
    }
}

impl FromStr for FileColumn {
    type Err = CoreError;

    fn from_str(input: &str) -> Result<FileColumn, Self::Err> {
        let str: &str = &input.to_ascii_lowercase();
        match str {
            "name"  => Ok(FileColumn::Name(None)),
            "path"  => Ok(FileColumn::Path(None)),
            "type"  => Ok(FileColumn::Type(None)),
            "file_extension" | "fileextension" => Ok(FileColumn::FileExtension(None)),
            "size" => Ok(FileColumn::Size(None)),
            "absolutepath" | "absolute_path" => Ok(FileColumn::AbsolutePath(None)),
            _ => Err(CoreError::GeneralError(format!("No type matching {} was found", str))),
        }
    }
}

impl Serialize for FileColumn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
            S: Serializer {
        let mut state = serializer.serialize_struct("Core File", 3)?;

        for column in FileColumn::iter() {
            state.serialize_field(column.as_static(), &self.to_string())?;
        }

        state.end()
    }
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

