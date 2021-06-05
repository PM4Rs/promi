use std::any::Any;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::stream::Artifact;
use crate::{DateTime, Error, Result};

/// Mirrors types available in `AttributeValue` enum
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AttributeType {
    String,
    Date,
    Int,
    Float,
    Boolean,
    Id,
    List,
}

/// Attribute value type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AttributeValue {
    String(String),
    Date(DateTime),
    Int(i64),
    Float(f64),
    Boolean(bool),
    Id(String),
    List(Vec<Attribute>),
}

impl AttributeValue {
    /// Try to cast attribute to string
    pub fn try_string(&self) -> Result<&str> {
        match self {
            AttributeValue::String(string) => Ok(string),
            other => Err(Error::AttributeError(format!("{:?} is no string", other))),
        }
    }

    /// Try to cast attribute to datetime
    pub fn try_date(&self) -> Result<&DateTime> {
        match self {
            AttributeValue::Date(timestamp) => Ok(timestamp),
            other => Err(Error::AttributeError(format!("{:?} is no datetime", other))),
        }
    }

    /// Try to cast attribute to integer
    pub fn try_int(&self) -> Result<&i64> {
        match self {
            AttributeValue::Int(integer) => Ok(integer),
            other => Err(Error::AttributeError(format!("{:?} is no integer", other))),
        }
    }

    /// Try to cast attribute to float
    pub fn try_float(&self) -> Result<&f64> {
        match self {
            AttributeValue::Float(float) => Ok(float),
            other => Err(Error::AttributeError(format!("{:?} is no float", other))),
        }
    }

    /// Try to cast attribute to boolean
    pub fn try_boolean(&self) -> Result<&bool> {
        match self {
            AttributeValue::Boolean(boolean) => Ok(boolean),
            other => Err(Error::AttributeError(format!("{:?} is no boolean", other))),
        }
    }

    /// Try to cast attribute to id
    pub fn try_id(&self) -> Result<&str> {
        match self {
            AttributeValue::Id(id) => Ok(id),
            other => Err(Error::AttributeError(format!("{:?} is no id", other))),
        }
    }

    /// Try to cast attribute to list
    pub fn try_list(&self) -> Result<&[Attribute]> {
        match self {
            AttributeValue::List(list) => Ok(list),
            other => Err(Error::AttributeError(format!("{:?} is no list", other))),
        }
    }

    /// Tell the caller what type this attribute value is of
    pub fn type_hint(&self) -> AttributeType {
        match self {
            AttributeValue::String(_) => AttributeType::String,
            AttributeValue::Date(_) => AttributeType::Date,
            AttributeValue::Int(_) => AttributeType::Int,
            AttributeValue::Float(_) => AttributeType::Float,
            AttributeValue::Boolean(_) => AttributeType::Boolean,
            AttributeValue::Id(_) => AttributeType::Id,
            AttributeValue::List(_) => AttributeType::List,
        }
    }
}

// Since both, `AttributeValue::String` and `AttributeValue::Id`, build up on String, only one of
// them can automatically converted. Since strings occurs more often and it's usage is more
// universal, we decided for this type.
impl From<&str> for AttributeValue {
    fn from(value: &str) -> Self {
        AttributeValue::String(value.into())
    }
}

impl From<String> for AttributeValue {
    fn from(value: String) -> Self {
        AttributeValue::String(value)
    }
}

impl From<DateTime> for AttributeValue {
    fn from(value: DateTime) -> Self {
        AttributeValue::Date(value)
    }
}

impl From<i64> for AttributeValue {
    fn from(value: i64) -> Self {
        AttributeValue::Int(value)
    }
}

impl From<f64> for AttributeValue {
    fn from(value: f64) -> Self {
        AttributeValue::Float(value)
    }
}

impl From<bool> for AttributeValue {
    fn from(value: bool) -> Self {
        AttributeValue::Boolean(value)
    }
}

// NOTE: `impl<T: IntoIter<Item=Attribute>` From <T>... would collide with `From<String>` :/
impl From<Vec<Attribute>> for AttributeValue {
    fn from(value: Vec<Attribute>) -> Self {
        AttributeValue::List(value)
    }
}

#[typetag::serde]
impl Artifact for AttributeValue {
    fn upcast_ref(&self) -> &dyn Any {
        self
    }

    fn upcast_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Express atomic information
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > Information on any component (log, trace, or event) is stored in attribute components.
/// > Attributes describe the enclosing component, which may contain an arbitrary number of
/// > attributes.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribute {
    pub key: String,
    pub value: AttributeValue,
}

impl Attribute {
    pub fn new<K: Into<String>, V: Into<AttributeValue>>(key: K, attribute: V) -> Attribute {
        Attribute {
            key: key.into(),
            value: attribute.into(),
        }
    }

    pub fn hint(&self) -> AttributeType {
        self.value.type_hint()
    }
}

#[typetag::serde]
impl Artifact for Attribute {
    fn upcast_ref(&self) -> &dyn Any {
        self
    }

    fn upcast_mut(&mut self) -> &mut dyn Any {
        self
    }
}
