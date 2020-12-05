use std::{cmp::Ordering, error::Error, str::FromStr, ffi::OsString, fmt::{Debug, Display}, fmt, fs::{self, DirEntry, File, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter};
use sqlparser::{parser::ParserError, ast::Query};
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};
use super::file::file_type::FileType;

pub trait FileColumnValue {
    fn column(&self, column: &FileColumn) -> FileColumn;
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

#[derive(Debug, AsStaticStr, EnumIter, PartialEq, PartialOrd, Hash, Eq, Ord)]
pub enum FileColumn {
    Name(Option<OsString>),
    Path(Option<PathBuf>),
    Type(Option<FileType>),
    FileExtension(Option<String>),
    Size(Option<u64>),
    AbsolutePath(Option<PathBuf>),
    Created(Option<u32>)
}

impl FileColumn {
    pub fn iterator() -> Iter<'static, FileColumn> {
        [FileColumn::Name(None), FileColumn::Path(None), FileColumn::Type(None), FileColumn::FileExtension(None), FileColumn::Size(None), FileColumn::AbsolutePath(None), FileColumn::Created(None)].iter()
    }
}

// impl PartialOrd for FilkeC {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }

impl FromStr for FileColumn {
    type Err = ();

    fn from_str(input: &str) -> Result<FileColumn, Self::Err> {
        let str: &str = &input.to_ascii_lowercase();
        match str {
            "name"  => Ok(FileColumn::Name(None)),
            "path"  => Ok(FileColumn::Path(None)),
            "type"  => Ok(FileColumn::Type(None)),
            "file_extension" | "fileextension" => Ok(FileColumn::FileExtension(None)),
            "size" => Ok(FileColumn::Size(None)),
            "absolutepath" | "absolute_path" => Ok(FileColumn::AbsolutePath(None)),
            _ => Err(()),
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

// #[derive(Debug, AsStaticStr, EnumIter)]
// pub enum Column {
//     Name,
//     Path,
//     Type,
//     FileExtension,
//     Size,
//     AbsolutePath,
//     Created
// }


// impl Column {
//     pub fn iterator() -> Iter<'static, Column> {
//         [Column::Name, Column::Path, Column::Type, Column::FileExtension, Column::Size, Column::AbsolutePath, Column::Created].iter()
//     }
// }

// impl FromStr for Column {
//     type Err = ();

//     fn from_str(input: &str) -> Result<Column, Self::Err> {
//         let str: &str = &input.to_ascii_lowercase();
//         match str {
//             "name"  => Ok(Column::Name),
//             "path"  => Ok(Column::Path),
//             "type"  => Ok(Column::Type),
//             "file_extension" | "fileextension" => Ok(Column::FileExtension),
//             "size" => Ok(Column::Size),
//             "absolutepath" | "absolute_path" => Ok(Column::AbsolutePath),
//             _ => Err(()),
//         }
//     }
// }


// impl fmt::Display for Column {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         Debug::fmt(self, f)
//     }
// }

