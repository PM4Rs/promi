use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::stream::{Attribute, AttributeValue};
use crate::{Error, Result};

/// Tells whether global/classifier target events or traces
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Scope {
    Event,
    Trace,
}

impl TryFrom<Option<String>> for Scope {
    type Error = Error;

    fn try_from(value: Option<String>) -> Result<Self> {
        if let Some(s) = value {
            match s.as_str() {
                "trace" => Ok(Scope::Trace),
                "event" => Ok(Scope::Event),
                other => Err(Self::Error::XesError(format!("Invalid scope: {:?}", other))),
            }
        } else {
            Ok(Scope::Event)
        }
    }
}

/// Extension declaration -- the actual behaviour is implemented via the `Extension` trait
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionDecl {
    pub name: String,
    pub prefix: String,
    pub uri: String,
}

/// Global attributes and defaults
///
/// Globals define attributes that have to be present in target scope and provide default values for
/// such. This may either target traces or events, regardless whether within a trace or not.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Global {
    pub scope: Scope,
    pub attributes: Vec<Attribute>,
}

impl Global {
    /// Validate any item that implements the `Attributes` trait without considering its scope
    pub fn validate(&self, component: &dyn Attributes) -> Result<()> {
        for attribute in self.attributes.iter() {
            if let Some(other) = component.get(&attribute.key) {
                if attribute.hint() != other.type_hint() {
                    return Err(Error::ValidationError(format!(
                        "Expected \"{:?}\" to be of type {:?} but got {:?} instead",
                        attribute.key,
                        attribute.hint(),
                        other.type_hint()
                    )));
                }
            } else {
                return Err(Error::ValidationError(format!(
                    "Couldn't find an attribute with key \"{:?}\"",
                    attribute.key
                )));
            }
        }
        Ok(())
    }
}

/// Classifier declaration -- the actual behaviour is implemented by `Classifier`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifierDecl {
    pub name: String,
    pub scope: Scope,
    pub keys: String,
}

/// Container for meta information of an event stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub extensions: Vec<ExtensionDecl>,
    pub globals: Vec<Global>,
    pub classifiers: Vec<ClassifierDecl>,
    pub attributes: BTreeMap<String, AttributeValue>,
}

impl Default for Meta {
    fn default() -> Self {
        Meta {
            extensions: Vec::new(),
            globals: Vec::new(),
            classifiers: Vec::new(),
            attributes: BTreeMap::new(),
        }
    }
}

/// Represents an atomic granule of activity that has been observed
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > An event component represents an atomic granule of activity that has been observed. If
/// > the event occurs in some trace, then it is clear to which case the event belongs. If the event
/// > does not occur in some trace, that is, if it occurs in the log, then we need ways to relate
/// > events to cases. For this, we will use the combination of a trace classifier and an event
/// > classifier.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub attributes: BTreeMap<String, AttributeValue>,
}

impl Default for Event {
    fn default() -> Self {
        Self {
            attributes: BTreeMap::new(),
        }
    }
}

/// Represents the execution of a single case
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > A trace component represents the execution of a single case, that is, of a single
/// > execution (or enactment) of the specific process. A trace shall contain a (possibly empty)
/// > list of events that are related to a single case. The order of the events in this list shall
/// > be important, as it signifies the order in which the events have been observed.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    pub attributes: BTreeMap<String, AttributeValue>,
    pub events: Vec<Event>,
}

impl Default for Trace {
    fn default() -> Self {
        Self {
            attributes: BTreeMap::new(),
            events: Vec::new(),
        }
    }
}

/// Atomic unit of an extensible event stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Component {
    Meta(Meta),
    Trace(Trace),
    Event(Event),
}

/// State of an extensible event stream
#[derive(Debug, PartialEq, PartialOrd, Clone, Serialize, Deserialize)]
pub enum ComponentType {
    Meta,
    Trace,
    Event,
}

// TODO: Once `ops::Try` lands in stable, replace ResOpt by something like:
// ```Rust
// enum ResOpt {
//     Component(Component),
//     Error(Error),
//     None
// }
// ```
/// Container for stream components that can express the empty components as well as errors
pub type ResOpt = Result<Option<Component>>;

/// Provide a unified way to access an component's attributes and those of potential child components
pub trait Attributes {
    /// Try to return an attribute by its key
    fn get(&self, key: &str) -> Option<&AttributeValue>;

    /// Try to return an attribute by its key and rise an error it the key is not present
    fn get_or(&self, key: &str) -> Result<&AttributeValue> {
        match self.get(key) {
            Some(value) => Ok(value),
            None => Err(Error::KeyError(key.to_string())),
        }
    }

    /// Access child components of this component
    fn children(&self) -> Vec<&dyn Attributes> {
        vec![]
    }

    /// Tell the caller what kind of object this view refers to
    fn hint(&self) -> ComponentType;
}

impl Attributes for Meta {
    fn get(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }

    fn hint(&self) -> ComponentType {
        ComponentType::Meta
    }
}

impl Attributes for Trace {
    fn get(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }

    fn children(&self) -> Vec<&dyn Attributes> {
        self.events.iter().map(|e| e as &dyn Attributes).collect()
    }

    fn hint(&self) -> ComponentType {
        ComponentType::Trace
    }
}

impl Attributes for Event {
    fn get(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }

    fn hint(&self) -> ComponentType {
        ComponentType::Event
    }
}

impl Attributes for Component {
    fn get(&self, key: &str) -> Option<&AttributeValue> {
        match self {
            Component::Meta(meta) => meta.get(key),
            Component::Trace(trace) => trace.get(key),
            Component::Event(event) => event.get(key),
        }
    }

    fn children(&self) -> Vec<&dyn Attributes> {
        match self {
            Component::Meta(meta) => meta.children(),
            Component::Trace(trace) => trace.children(),
            Component::Event(event) => event.children(),
        }
    }

    fn hint(&self) -> ComponentType {
        match self {
            Component::Meta(meta) => meta.hint(),
            Component::Trace(trace) => trace.hint(),
            Component::Event(event) => event.hint(),
        }
    }
}
