use std::{cmp::Ordering, error::Error, ffi::OsString, fmt::{Debug, Display}, fmt::{self, Formatter}, fs::{self, DirEntry, File, FileType, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter, str::FromStr};
use sqlparser::{ast::{BinaryOperator, Expr, Ident, Query}, parser::ParserError};
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};
use super::{column::Column, error::CoreError, value::CoreValue, column::FileColumn};

impl Error for CoreError {}


pub trait Visitor<T> {
    fn visit_expr(&mut self, e: &Expr) -> T;
}

// impl Visitor<CoreFile> for CoreFile { 
//     fn visit_expr(&mut self, e: &Expr) -> CoreFile {
//         match e {
//             Expr::Identifier(ident) => {
//                 unimplemented!()
//             }
//             Expr::BinaryOp { left, op, right } => {
//                 match op {
//                     BinaryOperator::Plus => { self.visit_expr(&left) + self.visit_expr(&right) }
//                     BinaryOperator::Eq => { 
//                         let left = self.visit_expr(&left);
//                         let bool = left == self.visit_expr(&right);
//                         left
//                     }
//                     _ => {unimplemented!() }
//                 }
//             }
//             _ => { unimplemented!() }
//         }       
//     }
// }

impl Visitor<FileColumn> for Option<&CoreFile> { 
    fn visit_expr(&mut self, e: &Expr) -> FileColumn {
        match e {
            Expr::Identifier(ident) => {
                unimplemented!()//self.value_absolute_path()
            }
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::Plus => { self.visit_expr(&left) + self.visit_expr(&right) }
                    BinaryOperator::Eq => { 
                        let left = self.visit_expr(&left);
                        if left != self.visit_expr(&right) {
                            *self = None   
                        }
                        left
                    }
                    _ => {unimplemented!() }
                }
            }
            _ => { unimplemented!() }
        }       
    }
}

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
    fn value(&self, column: &Column) -> FileColumn {
        match column {
            Column::Size => { self.value_size() }
            Column::Path => { self.value_path() }
            Column::AbsolutePath => { self.value_absolute_path() }
            Column::FileType => { self.value_file_type() }
            Column::FileExtension => { self.value_file_extension() }
            Column::Name => { self.value_name() }
            Column::Created => { self.value_created() }
        }
    }

    fn value_size(&self) -> FileColumn;
    fn value_absolute_path(&self) -> FileColumn;
    fn value_path(&self) -> FileColumn;
    fn value_file_type(&self) -> FileColumn;
    fn value_file_extension(&self) -> FileColumn;
    fn value_name(&self) -> FileColumn;
    fn value_created(&self) -> FileColumn;
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
    fn value_size(&self) -> FileColumn {
        match self.size() {
            Ok(size) => { FileColumn::Size(Some(size)) }
            Err(_) => { FileColumn::Size(None) }
        }
    }

    fn value_absolute_path(&self) -> FileColumn {
        match self.absolute_path() {
            Ok(path) => { FileColumn::AbsolutePath(Some(path)) }
            Err(_) => { FileColumn::AbsolutePath(None) }
        }
    }

    fn value_path(&self) -> FileColumn {
        match &self.path {
            Some(path) => { FileColumn::Path(Some(path.clone())) }
            None => { FileColumn::Path(None) }
        }
    }

    fn value_file_type(&self) -> FileColumn {
        FileColumn::FileType(None)
    }

    fn value_created(&self) -> FileColumn {
        FileColumn::Created(None)
        //format!("{:?}", self.metadata().unwrap().created().unwrap())
    }

    fn value_file_extension(&self) -> FileColumn {
        match &self.name {
            Some(name) => { FileColumn::FileExtension(Some(name.to_string_lossy().chars().rev().take_while(|c| *c != '.').collect::<String>())) }
            None => { FileColumn::FileExtension(None) }
        }
    }

    fn value_name(&self) -> FileColumn {
        match &self.name {
            Some(name) => { FileColumn::Name(Some(name.to_string_lossy().into_owned())) }
            None => { FileColumn::Name(None) }
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
