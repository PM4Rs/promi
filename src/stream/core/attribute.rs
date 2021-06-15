use std::any::Any;
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::stream::{Artifact, ComponentType};
use crate::{DateTime, Error, Result};

/// Mirrors types available in `AttributeValue` enum
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribute {
    pub key: String,
    pub value: AttributeValue,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<Attribute>,
}

impl Attribute {
    /// Instantiate a new attribute without children
    pub fn new<K, V>(key: K, attribute: V) -> Attribute
    where
        K: Into<String>,
        V: Into<AttributeValue>,
    {
        Attribute {
            key: key.into(),
            value: attribute.into(),
            children: Vec::new(),
        }
    }

    /// Instantiate a new attribute with children
    pub fn with_children<K, V, C>(key: K, attribute: V, children: C) -> Attribute
    where
        K: Into<String>,
        V: Into<AttributeValue>,
        C: IntoIterator<Item = Attribute>,
    {
        Attribute {
            key: key.into(),
            value: attribute.into(),
            children: children.into_iter().collect(),
        }
    }

    pub fn hint(&self) -> AttributeType {
        self.value.type_hint()
    }
}

impl<K, V> From<(K, V)> for Attribute
where
    K: Into<String>,
    V: Into<AttributeValue>,
{
    fn from(tuple: (K, V)) -> Self {
        let (key, value) = tuple;
        Self::new(key, value)
    }
}

impl<K, V, C> From<(K, V, C)> for Attribute
where
    K: Into<String>,
    V: Into<AttributeValue>,
    C: IntoIterator<Item = Attribute>,
{
    fn from(tuple: (K, V, C)) -> Self {
        let (key, value, children) = tuple;
        Self::with_children(key, value, children)
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

/// Helper struct that represents an attribute without its key
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AttributeMapEntry {
    #[serde(flatten)]
    value: AttributeValue,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    children: Vec<Attribute>,
}

impl AttributeMapEntry {
    /// Instantiate a new entry with children
    fn with_children<V, C>(value: V, children: C) -> Self
    where
        V: Into<AttributeValue>,
        C: IntoIterator<Item = Attribute>,
    {
        Self {
            value: value.into(),
            children: children.into_iter().collect(),
        }
    }
}

/// `BTreeMap` based container for attributes that allows for efficient access
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttributeMap {
    #[serde(flatten)]
    inner: BTreeMap<String, AttributeMapEntry>,
}

impl Default for AttributeMap {
    fn default() -> Self {
        Self::new()
    }
}

impl<A, I> From<I> for AttributeMap
where
    A: Into<Attribute>,
    I: Iterator<Item = A>,
{
    fn from(attributes: I) -> Self {
        Self {
            inner: attributes
                .into_iter()
                .map(|a| a.into())
                .map(|a| (a.key, AttributeMapEntry::with_children(a.value, a.children)))
                .collect(),
        }
    }
}

impl IntoIterator for AttributeMap {
    type Item = Attribute;
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(
            self.inner
                .into_iter()
                .map(|(k, e)| Attribute::with_children(k, e.value, e.children)),
        )
    }
}

impl Extend<Attribute> for AttributeMap {
    fn extend<T: IntoIterator<Item = Attribute>>(&mut self, iter: T) {
        iter.into_iter().for_each(|a| {
            self.insert(a);
        });
    }
}

impl AttributeMap {
    /// Instantiate an empty attribute map
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Insert a single attribute into the map
    pub fn insert<A>(&mut self, attribute: A)
    where
        A: Into<Attribute>,
    {
        let attribute: Attribute = attribute.into();
        self.inner.insert(
            attribute.key,
            AttributeMapEntry::with_children(attribute.value, attribute.children),
        );
    }

    /// Access value of an attribute
    pub fn get_value(&self, key: &str) -> Option<&AttributeValue> {
        self.inner.get(key).map(|e| &e.value)
    }

    /// Access mutable value of an attribute
    pub fn get_value_mut(&mut self, key: &str) -> Option<&mut AttributeValue> {
        self.inner.get_mut(key).map(|e| &mut e.value)
    }

    /// Access children of an attribute
    pub fn get_children(&self, key: &str) -> Option<&[Attribute]> {
        self.inner.get(key).map(|e| e.children.as_slice())
    }

    /// Access mutable children of an attribute
    pub fn get_children_mut(&mut self, key: &str) -> Option<&mut [Attribute]> {
        self.inner.get_mut(key).map(|e| e.children.as_mut_slice())
    }

    /// Remove attribute from map
    pub fn remove(&mut self, key: &str) -> Option<Attribute> {
        self.inner
            .remove_entry(key)
            .map(|(k, e)| Attribute::with_children(k, e.value, e.children))
    }

    /// Get the number of attributes in map
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check whether the map is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over entries
    pub fn iter(&self) -> AttributeMapIterator<'_> {
        return AttributeMapIterator {
            inner: self.inner.iter(),
        };
    }
}

/// An iterator over tuple representations of attributes
pub struct AttributeMapIterator<'a> {
    inner: Iter<'a, String, AttributeMapEntry>,
}

impl<'a> Iterator for AttributeMapIterator<'a> {
    type Item = (&'a str, &'a AttributeValue, &'a [Attribute]);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, e)| (k.as_str(), &e.value, e.children.as_slice()))
    }
}

/// Provide a unified way to access an component's attributes and those of potential child components
pub trait AttributeContainer {
    /// Try to return an attribute value by its key
    fn get_value(&self, key: &str) -> Option<&AttributeValue>;

    /// Try to return an attribute value by its key or rise an error it the key is not present
    fn get_value_or(&self, key: &str) -> Result<&AttributeValue> {
        match self.get_value(key) {
            Some(value) => Ok(value),
            None => Err(Error::KeyError(key.to_string())),
        }
    }

    /// Try to return an attribute's children by its key
    fn get_children(&self, key: &str) -> Option<&[Attribute]>;

    /// Try to return an attribute's children by its key or rise an error it the key is not present
    fn get_children_or(&self, key: &str) -> Result<&[Attribute]> {
        match self.get_children(key) {
            Some(value) => Ok(value),
            None => Err(Error::KeyError(key.to_string())),
        }
    }

    /// Access inner components of this component
    fn inner(&self) -> Vec<&dyn AttributeContainer> {
        vec![]
    }

    /// Tell the caller what kind of object this view refers to
    fn hint(&self) -> ComponentType;
}
