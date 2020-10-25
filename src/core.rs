
use std::{cmp::Ordering, error::Error, str::FromStr, ffi::OsString, fmt::Debug, fmt, fs::{self, DirEntry, File, FileType, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter};
use sqlparser::{parser::ParserError, ast::Query};
use serde::{Serialize, ser::SerializeStruct, Serializer};

#[derive(Debug, Clone, PartialEq)]
pub enum CoreError {
    IOError(String),
    ParserError(String),
    GeneralError(String),
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
            }
        )
    }
}

impl Error for CoreError {}

#[derive(Debug)]
pub struct CoreFile {
    pub name: Option<OsString>,
    pub path: Option<PathBuf>,
    pub file_type: Option<FileType>,
    pub file_extension: Option<String>,
}

impl Serialize for CoreFile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
            S: Serializer {
        let mut state = serializer.serialize_struct("Core File", 3)?;
        state.serialize_field("name", &self.name.as_ref().unwrap().to_str())?;
        state.serialize_field("path", &self.path)?;
        state.serialize_field("extension", &self.file_extension)?;
        state.end()
    }
}

#[derive(Debug)]
pub enum Column {
    Name,
    Path,
    FileType,
    FileExtension,
    Size,
    AbsolutePath
}

impl Column {
    pub fn iterator() -> Iter<'static, Column> {
        [Column::Name, Column::Path, Column::FileType, Column::FileExtension, Column::Size, Column::AbsolutePath].iter()
    }
}

impl FromStr for Column {
    type Err = ();

    fn from_str(input: &str) -> Result<Column, Self::Err> {
        let str: &str = &input.to_ascii_uppercase();
        match str {
            "name"  => Ok(Column::Name),
            "path"  => Ok(Column::Path),
            "filetype" | "file_type"  => Ok(Column::FileType),
            "file_extension" | "fileextension" => Ok(Column::FileExtension),
            "size" => Ok(Column::Size),
            "absolutepath" | "absolute_path" => Ok(Column::AbsolutePath),
            _      => Err(()),
        }
    }
}


impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl CoreFile {
    pub fn size(&self) -> Result<u64, io::Error> {
        match &self.path {
            Some(path) => { Ok(fs::metadata(&path)?.len()) }
            None => { Err(io::Error::new(io::ErrorKind::NotFound, "File not found")) }
        }
    }
    pub fn metadata(&self) -> Result<Metadata, io::Error> {
        match &self.path {
            Some(path) => { fs::metadata(&path) }
            None => { Err(io::Error::new(io::ErrorKind::NotFound, "File not found")) }
        }
    }
    pub fn absolute_path(&self) -> Result<PathBuf, io::Error> {
        Err(io::Error::new(io::ErrorKind::NotFound, "Unimplemented"))
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

// impl FromIterator for Vec<CoreFile> {
//     fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        
//     }
// }