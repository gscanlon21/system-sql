use std::{cmp::Ordering, error::Error, ffi::OsString, fmt::{Debug, Display}, fmt::{self, Formatter}, fs::{self, DirEntry, File, FileType, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter, str::FromStr};
use sqlparser::{ast::{BinaryOperator, Expr, Ident, Query}, parser::ParserError};
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};
use super::{column::Column, error::CoreError, column::FileColumn};

impl Error for CoreError {}

pub trait ColumnDisplay {
    fn display(&self, column: &Column) -> String {
        match column {
            Column::Size => { self.display_size() }
            Column::Path => { self.display_path() }
            Column::AbsolutePath => { self.display_absolute_path() }
            Column::FileType => { self.display_file_type() }
            Column::FileExtension => { self.display_file_extension() }
            Column::Name => { self.display_name() }
            Column::Created => { self.display_created() }
        }
    }

    fn display_size(&self) -> String;
    fn display_absolute_path(&self) -> String;
    fn display_path(&self) -> String;
    fn display_file_type(&self) -> String;
    fn display_file_extension(&self) -> String;
    fn display_name(&self) -> String;
    fn display_created(&self) -> String;
}

pub trait ColumnValue {
    fn value(&self, column: &Column) -> String {
        match column {
            Column::Size => { self.value_size().unwrap().to_string() }
            Column::Path => { self.value_path().unwrap().to_string_lossy().to_string() }
            Column::AbsolutePath => { self.value_absolute_path().unwrap().to_string_lossy().to_string() }
            Column::FileType => { self.value_file_type().unwrap() }
            Column::FileExtension => { self.value_file_extension().unwrap() }
            Column::Name => { self.value_name().unwrap() }
            Column::Created => { self.value_created().unwrap().to_string() }
        }
    }

    fn value_size(&self) -> Option<u64>;
    fn value_absolute_path(&self) -> Option<PathBuf>;
    fn value_path(&self) -> Option<PathBuf>;
    fn value_file_type(&self) -> Option<String>;
    fn value_file_extension(&self) -> Option<String>;
    fn value_name(&self) -> Option<String>;
    fn value_created(&self) -> Option<u64>;
}

trait ColumnGetter {
    fn size(&self) -> Result<u64, io::Error>;
    fn metadata(&self) -> Result<Metadata, io::Error>;
    fn absolute_path(&self) -> Result<PathBuf, io::Error>;
}

#[derive(Debug)]
pub struct CoreFile {
    pub name: Option<OsString>,
    pub path: Option<PathBuf>,
    pub file_type: Option<FileType>,
    pub file_extension: Option<String>,
}

impl CoreFile {
    fn size(&self) -> Result<u64, io::Error> {
        match &self.path {
            Some(path) => { Ok(fs::metadata(&path)?.len()) }
            None => { Err(io::Error::new(io::ErrorKind::NotFound, "File not found")) }
        }
    }
    fn metadata(&self) -> Result<Metadata, io::Error> {
        match &self.path {
            Some(path) => { fs::metadata(&path) }
            None => { Err(io::Error::new(io::ErrorKind::NotFound, "File not found")) }
        }
    }
    fn absolute_path(&self) -> Result<PathBuf, io::Error> {
        Err(io::Error::new(io::ErrorKind::NotFound, "Unimplemented"))
    }
}

impl Serialize for CoreFile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
            S: Serializer {
        let mut state = serializer.serialize_struct("Core File", 3)?;

        for column in Column::iter() {
            state.serialize_field(column.as_static(), &self.display(&column))?;
        }

        state.end()
    }
}

impl Display for CoreFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for column in Column::iter() {
            writeln!(f, "{}: {}", column, self.display(&column))?;
        }

        return Ok(());
    }
}


impl ColumnDisplay for CoreFile {
    fn display_size(&self) -> String {
        match self.size() {
            Ok(size) => { size.to_string() }
            Err(_) => { String::from("") }
        }
    }

    fn display_absolute_path(&self) -> String {
        match self.absolute_path() {
            Ok(path) => { path.clone().to_string_lossy().into_owned() }
            Err(_) => { String::from("") }
        }
    }

    fn display_path(&self) -> String {
        match &self.path {
            Some(path) => { path.to_string_lossy().into_owned() }
            None => { String::from("") }
        }
    }

    fn display_file_type(&self) -> String {
        "TODO".to_owned()
    }

    fn display_created(&self) -> String {
        format!("{:?}", self.metadata().unwrap().created().unwrap())
    }

    fn display_file_extension(&self) -> String {
        match &self.name {
            Some(name) => { name.to_string_lossy().chars().rev().take_while(|c| *c != '.').collect::<String>() }
            None => { String::from("") }
        }
    }

    fn display_name(&self) -> String {
        match &self.name {
            Some(name) => { name.to_string_lossy().into_owned() }
            None => { String::from("") }
        }
    }
}

impl ColumnValue for CoreFile {
    fn value_size(&self) -> Option<u64> {
        match self.size() {
            Ok(size) => { Some(size) }
            Err(_) => { None }
        }
    }

    fn value_absolute_path(&self) -> Option<PathBuf> {
        match self.absolute_path() {
            Ok(path) => { Some(path) }
            Err(_) => { None }
        }
    }

    fn value_path(&self) -> Option<PathBuf> {
        match &self.path {
            Some(path) => { Some(path.clone()) }
            None => { None }
        }
    }

    fn value_file_type(&self) -> Option<String> {
        None
    }

    fn value_created(&self) -> Option<u64> {
        None
    }

    fn value_file_extension(&self) -> Option<String> {
        match &self.name {
            Some(name) => { Some(name.to_string_lossy().chars().rev().take_while(|c| *c != '.').collect::<String>()) }
            None => { None }
        }
    }

    fn value_name(&self) -> Option<String> {
        match &self.name {
            Some(name) => { Some(name.to_string_lossy().into_owned()) }
            None => { None }
        }
    }
}

impl Ord for CoreFile {
    fn cmp(&self, other: &Self) -> Ordering {
        self.path.cmp(&other.path)
    }
}

impl PartialOrd for CoreFile {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CoreFile {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for CoreFile {}

impl From<DirEntry> for CoreFile {
    fn from(dir: DirEntry) -> Self {
        CoreFile {
            path: Some(dir.path()),
            name: Some(dir.file_name()),
            file_type: dir.file_type().ok(),
            file_extension: None
        }
    }
}

