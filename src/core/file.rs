use std::{cmp::Ordering, error::Error, ffi::OsString, fmt::{Debug, Display}, fmt::{self, Formatter}, fs::{self, DirEntry, File, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter, str::FromStr};
use sqlparser::{ast::{BinaryOperator, Expr, Ident, Query}, parser::ParserError};
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};
use self::file_type::FileType;
use super::{error::CoreError, column::*};

pub mod file_type {
    #[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq, Ord)]
    pub enum FileType {
        File,
        Dir,
    }
    
    impl From<std::fs::FileType> for FileType {
        fn from(file_type: std::fs::FileType) -> Self {
            if file_type.is_file() {
                FileType::File
            } else {
                FileType::Dir
            }
        }
    }

    impl std::fmt::Display for FileType {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(
                f,
                "Error: {}",
                match self {
                    FileType::File => "file",
                    FileType::Dir => "directory",
                }
            )
        }
    }
}
 
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct CoreFile {
    pub name: Option<OsString>,
    pub path: Option<PathBuf>,
    pub file_type: Option<FileType>,
    pub file_extension: Option<String>,
}

impl CoreFile {
    fn metadata(&self) -> Result<Metadata, io::Error> {
        match &self.path {
            Some(path) => { fs::metadata(&path) }
            None => { Err(io::Error::new(io::ErrorKind::NotFound, "File not found")) }
        }
    }
}

impl FileColumnValues for CoreFile {
    fn size(&self) -> FileColumn {
        FileColumn::Size(match &self.path {
            Some(path) => { 
                if let Ok(metadata) = fs::metadata(&path) {
                    Some(metadata.len())
                } else { None }
            }
            None => { None }
        })
    }
    
    fn absolute_path(&self) -> FileColumn {
        todo!()
    }
    fn name(&self) -> FileColumn {
        FileColumn::Name(self.name.clone())
    }

    fn path(&self) -> FileColumn {
        FileColumn::Path(self.path.clone())
    }

    fn file_type(&self) -> FileColumn {
        FileColumn::Type(if let Ok(metadata) = self.metadata() {
            if metadata.is_file() {
                Some(FileType::File)
            } else {
                Some(FileType::Dir)
            }
        } else { None })
    }

    fn file_extension(&self) -> FileColumn {
        todo!()
    }

    fn created(&self) -> FileColumn {
        todo!()
    }
}

impl FileColumnValue for CoreFile {
    fn column(&self, column: &FileColumn) -> FileColumn {
        match column {
            FileColumn::Name(_) => { self.name() }
            FileColumn::Path(_) => { self.path() }
            FileColumn::Type(_) => { self.file_type() }
            FileColumn::FileExtension(_) => { self.file_extension() }
            FileColumn::Size(_) => { self.size() }
            FileColumn::AbsolutePath(_) => { self.absolute_path() }
            FileColumn::Created(_) => { self.created() }
        }
    }
}

impl Serialize for CoreFile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
            S: Serializer {
        let mut state = serializer.serialize_struct("Core File", 3)?;

        for column in FileColumn::iter() {
            state.serialize_field(column.as_static(), &self.column(&column).to_string())?;
        }

        state.end()
    }
}

impl Display for CoreFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for column in FileColumn::iter() {
            writeln!(f, "{}: {}", column, self.column(&column).to_string())?;
        }

        return Ok(());
    }
}

impl From<DirEntry> for CoreFile {
    fn from(dir: DirEntry) -> Self {
        CoreFile {
            path: Some(dir.path()),
            name: Some(dir.file_name()),
            file_type: if let Ok(file_type) = dir.file_type() { Some(FileType::from(file_type)) } else { None } ,
            file_extension: None
        }
    }
}

