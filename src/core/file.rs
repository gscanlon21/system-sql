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

// pub trait ColumnDisplay {
//     fn display(&self, column: &FileColumn) -> String {
//         match column {
//             FileColumn::Size(_) => { self.display_size() }
//             FileColumn::Path(_) => { self.display_path() }
//             FileColumn::AbsolutePath(_) => { self.display_absolute_path() }
//             FileColumn::Type(_) => { self.display_type() }
//             FileColumn::FileExtension(_) => { self.display_file_extension() }
//             FileColumn::Name(_) => { self.display_name() }
//             FileColumn::Created(_) => { self.display_created() }
//         }
//     }

//     fn display_size(&self) -> String;
//     fn display_absolute_path(&self) -> String;
//     fn display_path(&self) -> String;
//     fn display_type(&self) -> String;
//     fn display_file_extension(&self) -> String;
//     fn display_name(&self) -> String;
//     fn display_created(&self) -> String;
// }

// pub trait ColumnValue {
//     fn value(&self, column: &FileColumn) -> String {
//         match column {
//             FileColumn::Size(_) => { self.value_size().unwrap().to_string() }
//             FileColumn::Path(_) => { self.value_path().unwrap().to_string_lossy().to_string() }
//             FileColumn::Type(_) => { if self.value_type().unwrap().is_file() { "file".to_owned() } else { "dir".to_owned() } }
//             FileColumn::AbsolutePath(_) => { self.value_absolute_path().unwrap().to_string_lossy().to_string() }
//             FileColumn::FileExtension(_) => { self.value_file_extension().unwrap() }
//             FileColumn::Name(_) => { self.value_name().unwrap() }
//             FileColumn::Created(_) => { self.value_created().unwrap().to_string() }
//         }
//     }

//     fn value_size(&self) -> Option<u64>;
//     fn value_absolute_path(&self) -> Option<PathBuf>;
//     fn value_path(&self) -> Option<PathBuf>;
//     fn value_type(&self) -> Option<fs::FileType>;
//     fn value_file_extension(&self) -> Option<String>;
//     fn value_name(&self) -> Option<String>;
//     fn value_created(&self) -> Option<u64>;
// }
 
#[derive(Debug, Clone)]
pub struct CoreFile {
    pub name: Option<OsString>,
    pub path: Option<PathBuf>,
    pub file_type: Option<FileType>,
    pub file_extension: Option<String>,
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

impl CoreFile {
    fn metadata(&self) -> Result<Metadata, io::Error> {
        match &self.path {
            Some(path) => { fs::metadata(&path) }
            None => { Err(io::Error::new(io::ErrorKind::NotFound, "File not found")) }
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


// impl ColumnDisplay for CoreFile {
//     fn display_size(&self) -> String {
//         match self.size() {
//             Ok(size) => { size.to_string() }
//             Err(_) => { String::from("") }
//         }
//     }

//     fn display_absolute_path(&self) -> String {
//         match self.absolute_path() {
//             Ok(path) => { path.clone().to_string_lossy().into_owned() }
//             Err(_) => { String::from("") }
//         }
//     }

//     fn display_path(&self) -> String {
//         match &self.path {
//             Some(path) => { path.to_string_lossy().into_owned() }
//             None => { String::from("") }
//         }
//     }

//     fn display_type(&self) -> String {
//         if self.metadata().unwrap().file_type().is_file() { "file".to_owned() } else { "dir".to_owned() }
//     }

//     fn display_created(&self) -> String {
//         format!("{:?}", self.metadata().unwrap().created().unwrap())
//     }

//     fn display_file_extension(&self) -> String {
//         match &self.name {
//             Some(name) => { name.to_string_lossy().chars().rev().take_while(|c| *c != '.').collect::<String>() }
//             None => { String::from("") }
//         }
//     }

//     fn display_name(&self) -> String {
//         match &self.name {
//             Some(name) => { name.to_string_lossy().into_owned() }
//             None => { String::from("") }
//         }
//     }
// }

// impl ColumnValue for CoreFile {
//     fn value_size(&self) -> Option<u64> {
//         match self.size() {
//             Ok(size) => { Some(size) }
//             Err(_) => { None }
//         }
//     }

//     fn value_absolute_path(&self) -> Option<PathBuf> {
//         match self.absolute_path() {
//             Ok(path) => { Some(path) }
//             Err(_) => { None }
//         }
//     }

//     fn value_path(&self) -> Option<PathBuf> {
//         match &self.path {
//             Some(path) => { Some(path.clone()) }
//             None => { None }
//         }
//     }

//     fn value_type(&self) -> Option<fs::FileType> {
//         Some(self.metadata().unwrap().file_type())
//     }

//     fn value_created(&self) -> Option<u64> {
//         None
//     }

//     fn value_file_extension(&self) -> Option<String> {
//         match &self.name {
//             Some(name) => { Some(name.to_string_lossy().chars().rev().take_while(|c| *c != '.').collect::<String>()) }
//             None => { None }
//         }
//     }

//     fn value_name(&self) -> Option<String> {
//         match &self.name {
//             Some(name) => { Some(name.to_string_lossy().into_owned()) }
//             None => { None }
//         }
//     }
// }

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
            file_type: if let Ok(file_type) = dir.file_type() { Some(FileType::from(file_type)) } else { None } ,
            file_extension: None
        }
    }
}

