use std::convert::TryFrom;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::stream::{Attribute, AttributeContainer, AttributeMap, AttributeValue};
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
    pub fn validate(&self, component: &dyn AttributeContainer) -> Result<()> {
        for attribute in self.attributes.iter() {
            if let Some(other) = component.get_value(&attribute.key) {
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
    pub attributes: AttributeMap,
}

impl Default for Meta {
    fn default() -> Self {
        Meta {
            extensions: Vec::new(),
            globals: Vec::new(),
            classifiers: Vec::new(),
            attributes: AttributeMap::new(),
        }
    }
}

impl AttributeContainer for Meta {
    fn get_value(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get_value(key)
    }

    fn get_children(&self, key: &str) -> Option<&[Attribute]> {
        self.attributes.get_children(key)
    }

    fn hint(&self) -> ComponentType {
        ComponentType::Meta
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
    pub attributes: AttributeMap,
}

impl Default for Event {
    fn default() -> Self {
        Self {
            attributes: AttributeMap::new(),
        }
    }
}

impl AttributeContainer for Event {
    fn get_value(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get_value(key)
    }

    fn get_children(&self, key: &str) -> Option<&[Attribute]> {
        self.attributes.get_children(key)
    }

    fn hint(&self) -> ComponentType {
        ComponentType::Event
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
    pub attributes: AttributeMap,
    pub events: Vec<Event>,
}

impl Default for Trace {
    fn default() -> Self {
        Self {
            attributes: AttributeMap::new(),
            events: Vec::new(),
        }
    }
}

impl AttributeContainer for Trace {
    fn get_value(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get_value(key)
    }

    fn get_children(&self, key: &str) -> Option<&[Attribute]> {
        self.attributes.get_children(key)
    }

    fn inner(&self) -> Vec<&dyn AttributeContainer> {
        self.events
            .iter()
            .map(|e| e as &dyn AttributeContainer)
            .collect()
    }

    fn hint(&self) -> ComponentType {
        ComponentType::Trace
    }
}

/// State of an extensible event stream
#[derive(Debug, PartialEq, PartialOrd, Clone, Serialize, Deserialize)]
pub enum ComponentType {
    Meta,
    Trace,
    Event,
}

/// Atomic unit of an extensible event stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Component {
    Meta(Meta),
    Trace(Trace),
    Event(Event),
}

impl AttributeContainer for Component {
    fn get_value(&self, key: &str) -> Option<&AttributeValue> {
        match self {
            Component::Meta(meta) => meta.get_value(key),
            Component::Trace(trace) => trace.get_value(key),
            Component::Event(event) => event.get_value(key),
        }
    }

    fn get_children(&self, key: &str) -> Option<&[Attribute]> {
        match self {
            Component::Meta(meta) => meta.get_children(key),
            Component::Trace(trace) => trace.get_children(key),
            Component::Event(event) => event.get_children(key),
        }
    }

    fn inner(&self) -> Vec<&dyn AttributeContainer> {
        match self {
            Component::Meta(meta) => meta.inner(),
            Component::Trace(trace) => trace.inner(),
            Component::Event(event) => event.inner(),
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
