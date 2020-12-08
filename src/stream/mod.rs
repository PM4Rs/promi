//! Extensible event streams
//!
//! This module provides a protocol for event streaming. Its design is strongly inspired by the
//! XES standard\[1\] (see also [xes-standard.org](http://www.xes-standard.org)).
//!
//! Further, it comes with commonly used convenience features such as buffering, filtering, basic
//! statistics, validation and (de-)serialization. The provided API allows you to easily build and
//! customize your data processing pipeline.
//!
//! \[1\] [_IEEE Standard for eXtensible Event Stream (XES) for Achieving Interoperability in Event
//! Logs and Event Streams_, 1849:2016, 2016](https://standards.ieee.org/standard/1849-2016.html)
//!

use std::any::Any;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Debug;

use erased_serde::{Serialize as ErasedSerialize, Serializer as ErasedSerializer};
use serde::{Deserialize, Serialize};

use crate::{DateTime, Error, Result};

// modules
pub mod buffer;
pub mod channel;
pub mod duplicator;
pub mod extension;
pub mod filter;
pub mod log;
pub mod observer;
pub mod pipe;
pub mod plugin;
pub mod repair;
pub mod split;
pub mod stats;
pub mod validator;
pub mod void;
pub mod xes;
pub mod xml_util;

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
            other => Err(Error::AttributeError(format!("{:?} is no integer", other))),
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
    pub fn hint(&self) -> AttributeType {
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

/// Express atomic information
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > Information on any component (log, trace, or event) is stored in attribute components.
/// > Attributes describe the enclosing component, which may contain an arbitrary number of
/// > attributes.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribute {
    key: String,
    value: AttributeValue,
}

impl Attribute {
    pub fn new<K: Into<String>>(key: K, attribute: AttributeValue) -> Attribute {
        Attribute {
            key: key.into(),
            value: attribute,
        }
    }

    fn hint(&self) -> AttributeType {
        self.value.hint()
    }
}

/// Represents whether global or classifier target events or traces
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
    name: String,
    prefix: String,
    uri: String,
}

/// Global attributes and defaults
///
/// Globals define attributes that have to be present in target scope and provide default values for
/// such. This may either target traces or events, regardless whether within a trace or not.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Global {
    scope: Scope,
    attributes: Vec<Attribute>,
}

impl Global {
    /// Validate any item that implements the `Attributes` trait without considering its scope
    pub fn validate(&self, component: &dyn Attributes) -> Result<()> {
        for attribute in self.attributes.iter() {
            if let Some(other) = component.get(&attribute.key) {
                if attribute.hint() != other.hint() {
                    return Err(Error::ValidationError(format!(
                        "Expected \"{:?}\" to be of type {:?} but got {:?} instead",
                        attribute.key,
                        attribute.hint(),
                        other.hint()
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
    name: String,
    scope: Scope,
    keys: String,
}

/// Container for meta information of an event stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    extensions: Vec<ExtensionDecl>,
    globals: Vec<Global>,
    classifiers: Vec<ClassifierDecl>,
    attributes: BTreeMap<String, AttributeValue>,
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
    attributes: BTreeMap<String, AttributeValue>,
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
    attributes: BTreeMap<String, AttributeValue>,
    events: Vec<Event>,
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
    fn children<'a>(&'a self) -> Vec<&'a dyn Attributes> {
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

    fn children<'a>(&'a self) -> Vec<&'a dyn Attributes> {
        self.events
            .iter()
            .map(|e| e as &'a dyn Attributes)
            .collect()
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

    fn children<'a>(&'a self) -> Vec<&'a dyn Attributes> {
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

/// A protocol to represent any kind of aggregation product a event stream may produce
pub trait Artifact: Any + Send + Debug + ErasedSerialize {
    fn as_any(&self) -> &dyn Any;

    fn as_any_mut(&mut self) -> &mut dyn Any;
}

erased_serde::serialize_trait_object!(Artifact);

/// Container for arbitrary artifacts a stream processing pipeline may create
#[derive(Debug, Serialize)]
pub struct AnyArtifact {
    artifact: Box<dyn Artifact>,
}

impl AnyArtifact {
    /// Try to cast down the artifact to the given type
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        Any::downcast_ref::<T>(self.artifact.as_any())
    }

    /// Try to cast down the artifact mutably to the given type
    pub fn downcast_mut<T: 'static>(&mut self) -> Option<&mut T> {
        Any::downcast_mut::<T>(self.artifact.as_any_mut())
    }

    /// Find the first artifact in an iterator that can be casted down to the given type
    pub fn find<'a, T: 'static>(
        artifacts: &mut dyn Iterator<Item = &'a AnyArtifact>,
    ) -> Option<&'a T> {
        for artifact in artifacts {
            if let Some(value) = artifact.downcast_ref::<T>() {
                return Some(value);
            }
        }
        None
    }

    /// Find all artifacts in an iterator that can be casted down to the given type
    pub fn find_all<'a, T: 'static>(
        artifacts: &'a mut (dyn std::iter::Iterator<Item = &'a AnyArtifact> + 'a),
    ) -> impl Iterator<Item = &'a T> {
        artifacts.filter_map(|a| a.downcast_ref::<T>())
    }

    /// Serialize inner artifact without the `AnyArtifact` container
    pub fn serialize_inner(&self, serializer: &mut dyn ErasedSerializer) -> Result<()> {
        Ok(self.artifact.erased_serialize(serializer).map(|_| ())?)
    }
}

impl<T: Artifact> From<T> for AnyArtifact {
    fn from(artifact: T) -> Self {
        AnyArtifact {
            artifact: Box::new(artifact),
        }
    }
}

/// Extensible event stream
///
/// Yields one stream component at a time. Usually, it either acts as a factory or forwards another
/// stream. Errors are propagated to the caller.
///
pub trait Stream: Send {
    /// Get a reference to the inner stream if there is one
    fn inner_ref(&self) -> Option<&dyn Stream>;

    /// Get a mutable reference to the inner stream if there is one
    fn inner_mut(&mut self) -> Option<&mut dyn Stream>;

    /// Return the next stream component
    fn next(&mut self) -> ResOpt;

    /// Callback that releases artifacts of stream
    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        Ok(vec![])
    }

    /// Emit artifacts of stream
    ///
    /// A stream may aggregate data over time that is released by calling this method. Usually,
    /// this happens at the end of the stream.
    ///
    fn emit_artifacts(&mut self) -> Result<Vec<Vec<AnyArtifact>>> {
        let mut artifacts = Vec::new();
        if let Some(inner) = self.inner_mut() {
            artifacts.extend(Stream::emit_artifacts(inner)?);
        }
        artifacts.push(Stream::on_emit_artifacts(self)?);
        Ok(artifacts)
    }

    /// Turn stream instance into trait object
    fn into_boxed<'a>(self) -> Box<dyn Stream + 'a>
    where
        Self: Sized + 'a,
    {
        Box::new(self)
    }
}

impl<'a> Stream for Box<dyn Stream + 'a> {
    fn inner_ref(&self) -> Option<&dyn Stream> {
        self.as_ref().inner_ref()
    }

    fn inner_mut(&mut self) -> Option<&mut dyn Stream> {
        self.as_mut().inner_mut()
    }

    fn next(&mut self) -> ResOpt {
        self.as_mut().next()
    }

    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        self.as_mut().on_emit_artifacts()
    }
}

/// Stream endpoint
///
/// A stream sink acts as an endpoint for an extensible event stream and is usually used when a
/// stream is converted into different representation. If that's not intended, the `void::consume`
/// function is a good shortcut. It simply discards the stream's contents.
///
pub trait Sink: Send {
    /// Optional callback  that is invoked when the stream is opened
    fn on_open(&mut self) -> Result<()> {
        Ok(())
    }

    /// Callback that is invoked on each stream component
    fn on_component(&mut self, _component: Component) -> Result<()> {
        Ok(())
    }

    /// Optional callback that is invoked once the stream is closed
    fn on_close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Optional callback that is invoked when an error occurs
    fn on_error(&mut self, _error: Error) -> Result<()> {
        Ok(())
    }

    /// Emit artifacts of stream sink
    ///
    /// A stream sink may aggregate data over time that is released by calling this method. Usually,
    /// this happens at the end of the stream.
    ///
    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        Ok(vec![])
    }

    /// Invokes a stream as long as it provides new components.
    fn consume(&mut self, stream: &mut dyn Stream) -> Result<Vec<Vec<AnyArtifact>>> {
        // call pre-execution hook
        self.on_open()?;

        // consume stream
        loop {
            match stream.next() {
                Ok(Some(component)) => self.on_component(component)?,
                Ok(None) => break,
                Err(error) => {
                    self.on_error(error.clone())?;
                    return Err(error);
                }
            };
        }

        // call post-execution hook
        self.on_close()?;

        // collect artifacts
        let mut artifacts = Stream::emit_artifacts(stream)?;
        artifacts.push(Sink::on_emit_artifacts(self)?);
        Ok(artifacts)
    }

    /// Turn sink instance into trait object
    fn into_boxed<'a>(self) -> Box<dyn Sink + 'a>
    where
        Self: Sized + 'a,
    {
        Box::new(self)
    }
}

impl<'a> Sink for Box<dyn Sink + 'a> {
    fn on_open(&mut self) -> Result<()> {
        self.as_mut().on_open()
    }

    fn on_component(&mut self, component: Component) -> Result<()> {
        self.as_mut().on_component(component)
    }

    fn on_close(&mut self) -> Result<()> {
        self.as_mut().on_close()
    }

    fn on_error(&mut self, error: Error) -> Result<()> {
        self.as_mut().on_error(error)
    }

    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        self.as_mut().on_emit_artifacts()
    }
}

#[cfg(test)]
mod tests {
    use crate::dev_util::load_example;

    use super::*;

    #[test]
    pub fn test_consume() {
        let mut buffer = load_example(&["book", "L1.xes"]);

        assert_eq!(buffer.len(), 7);

        void::consume(&mut buffer).unwrap();

        assert_eq!(buffer.len(), 0);
    }

    #[derive(Debug)]
    pub struct TestSink {
        ct_open: usize,
        ct_component: usize,
        ct_close: usize,
        ct_error: usize,
    }

    impl Default for TestSink {
        fn default() -> Self {
            TestSink {
                ct_open: 0,
                ct_component: 0,
                ct_close: 0,
                ct_error: 0,
            }
        }
    }

    impl Sink for TestSink {
        fn on_open(&mut self) -> Result<()> {
            self.ct_open += 1;
            Ok(())
        }

        fn on_component(&mut self, _: Component) -> Result<()> {
            self.ct_component += 1;
            Ok(())
        }

        fn on_close(&mut self) -> Result<()> {
            self.ct_close += 1;
            Ok(())
        }

        fn on_error(&mut self, _: Error) -> Result<()> {
            self.ct_error += 1;
            Ok(())
        }
    }

    impl TestSink {
        pub fn counts(&self) -> [usize; 4] {
            [
                self.ct_open,
                self.ct_component,
                self.ct_close,
                self.ct_error,
            ]
        }
    }
}
