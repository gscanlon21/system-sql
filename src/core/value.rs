use std::{cmp::Ordering, error::Error, str::FromStr, ffi::OsString, fmt::{Debug, Display}, fmt, fs::{self, DirEntry, File, FileType, Metadata}, io, iter::FromIterator, path::PathBuf, slice::Iter};
use sqlparser::{parser::ParserError, ast::Query};
use serde::{Serialize, ser::SerializeStruct, Serializer};
use strum::{AsStaticRef, IntoEnumIterator};

#[derive(Debug, Clone)]
pub enum CoreValue {
    None,
    String(String),
    Number(usize),
    Bool(bool),
}

impl From<String> for CoreValue {
    fn from(str: String) -> Self {
        CoreValue::String(str)
    }
}

impl From<bool> for CoreValue
{
    fn from(b: bool) -> Self {
        CoreValue::Bool(b)
    }
}

impl From<usize> for CoreValue
{
    fn from(u: usize) -> Self {
        CoreValue::Number(u)
    }
}

impl std::ops::Add for CoreValue
{
    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (CoreValue::String(a), CoreValue::String(b)) => CoreValue::from(a + &b),
            (CoreValue::Number(a), CoreValue::Number(b)) => CoreValue::from(a + b),
            _ => CoreValue::None,
        }
    }

    type Output = Self;
}

impl std::ops::Sub for CoreValue
{
    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (CoreValue::String(a), CoreValue::String(b)) => None,
            (CoreValue::Number(a), CoreValue::Number(b)) => Some(CoreValue::from(a - b)),
            _ => None,
        }
    }

    type Output = Option<Self>;
}

impl std::ops::Mul for CoreValue
{
    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (CoreValue::String(a), CoreValue::String(b)) => None,
            (CoreValue::Number(a), CoreValue::Number(b)) => Some(CoreValue::from(a * b)),
            _ => None,
        }
    }

    type Output = Option<Self>;
}

impl std::ops::Div for CoreValue
{
    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (CoreValue::String(a), CoreValue::String(b)) => None,
            (CoreValue::Number(a), CoreValue::Number(b)) => Some(CoreValue::from(a / b)),
            _ => None,
        }
    }

    type Output = Option<Self>;
}

impl PartialEq for CoreValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CoreValue::String(a), CoreValue::String(b)) => a == b,
            (CoreValue::Number(a), CoreValue::Number(b)) => a == b,
            _ => false,
        }
    }
}