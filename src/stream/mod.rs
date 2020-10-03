//! Extensible event streams
//!
//! This module provides a protocol for event streaming. Its design is strongly inspired by the
//! XES standard[1] (see also [xes-standard.org](http://www.xes-standard.org)).
//!
//! Further, it comes with commonly used convenience features such as buffering, filtering, basic
//! statistics, validation and (de-)serialization. The provided API allows you to easily build and
//! customize your data processing pipeline.
//!
//! [1] [_IEEE Standard for eXtensible Event Stream (XES) for Achieving Interoperability in Event
//! Logs and Event Streams_, 1849:2016, 2016](https://standards.ieee.org/standard/1849-2016.html)
//!

use std::any::Any;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Debug;

use crate::{DateTime, Error, Result};

// modules
pub mod buffer;
pub mod channel;
pub mod duplicator;
pub mod extension;
pub mod filter;
pub mod observer;
pub mod split;
pub mod stats;
pub mod validator;
pub mod xes;
pub mod xml_util;

/// Mirrors types available in `AttributeValue` enum
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone)]
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
    pub fn try_string(&self) -> Result<&String> {
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
    pub fn try_id(&self) -> Result<&String> {
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
#[derive(Debug, Clone)]
pub struct Attribute {
    key: String,
    value: AttributeValue,
}

impl Attribute {
    fn new(key: String, attribute: AttributeValue) -> Attribute {
        Attribute {
            key,
            value: attribute,
        }
    }

    fn hint(&self) -> AttributeType {
        self.value.hint()
    }
}

/// Represents whether global or classifier target events or traces
#[derive(Debug, Clone, Eq, PartialEq)]
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct ClassifierDecl {
    name: String,
    scope: Scope,
    keys: String,
}

/// Container for meta information of an event stream
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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

/// Represents information that is related to a specific process
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > A log component represents information that is related to a specific process. Examples
/// > of processes include handling insurance claims, using a complex X-ray machine, and browsing a
/// > website. A log shall contain a (possibly empty) collection of traces followed by a (possibly
/// > empty) list of events. The order of the events in this list shall be important, as it
/// > signifies the order in which the events have been observed. If the log contains only events
/// > and no traces, then the log is also called a stream.
///
#[derive(Debug, Clone)]
pub struct Log {
    meta: Meta,
    traces: Vec<Trace>,
    events: Vec<Event>,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            meta: Meta::default(),
            traces: Vec::new(),
            events: Vec::new(),
        }
    }
}

impl Into<buffer::Buffer> for Log {
    fn into(self) -> buffer::Buffer {
        let mut buffer = buffer::Buffer::default();

        buffer.push(Ok(Some(Component::Meta(self.meta))));

        for trace in self.traces {
            buffer.push(Ok(Some(Component::Trace(trace))));
        }

        for event in self.events {
            buffer.push(Ok(Some(Component::Event(event))));
        }

        buffer
    }
}

impl StreamSink for Log {
    fn on_component(&mut self, component: Component) -> Result<()> {
        match component {
            Component::Meta(meta) => self.meta = meta,
            Component::Trace(trace) => self.traces.push(trace),
            Component::Event(event) => self.events.push(event),
        };

        Ok(())
    }
}

/// Atomic unit of an extensible event stream
#[derive(Debug, Clone)]
pub enum Component {
    Meta(Meta),
    Trace(Trace),
    Event(Event),
}

/// State of an extensible event stream
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum ComponentType {
    Meta,
    Trace,
    Event,
}

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

impl Attributes for Log {
    fn get(&self, key: &str) -> Option<&AttributeValue> {
        self.meta.attributes.get(key)
    }

    fn children<'a>(&'a self) -> Vec<&'a dyn Attributes> {
        self.traces
            .iter()
            .map(|t| t as &'a dyn Attributes)
            .chain(self.events.iter().map(|e| e as &'a dyn Attributes))
            .collect()
    }

    fn hint(&self) -> ComponentType {
        ComponentType::Meta
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

/// Container for arbitrary artifacts a stream processing pipeline creates
#[derive(Debug)]
pub struct Artifact {
    inner: Box<dyn Any + Send>,
}

impl Artifact {
    /// Tries to down cast the artifact to the given type
    pub fn cast_ref<T: 'static>(&self) -> Option<&T> {
        self.inner.downcast_ref::<T>()
    }

    /// Tries to cast down the artifact mutably to the given type
    pub fn cast_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.inner.downcast_mut::<T>()
    }

    /// Returns the first artifact that can be casted down to the given type
    pub fn find<T: 'static>(artifacts: &[Artifact]) -> Option<&T> {
        for artifact in artifacts {
            if let Some(value) = artifact.cast_ref::<T>() {
                return Some(value);
            }
        }
        None
    }

    /// Returns all artifacts that can be casted down to the given type
    pub fn find_all<T: 'static>(artifacts: &[Artifact]) -> Box<dyn Iterator<Item = &T> + '_> {
        Box::new(artifacts.iter().filter_map(|a| a.cast_ref::<T>()))
    }
}

impl Artifact {
    /// Create new artifact from any given item
    pub fn new<T: Any + Send + 'static>(t: T) -> Self {
        Self { inner: Box::new(t) }
    }
}

/// Extensible event stream
///
/// Yields one stream component at a time. Usually, it either acts as a factory or forwards another
/// stream. Errors are propagated to the caller.
///
pub trait Stream: Send {
    /// Get a reference to the inner stream if there is one
    fn get_inner(&self) -> Option<&dyn Stream>;

    /// Get a mutable reference to the inner stream if there is one
    fn get_inner_mut(&mut self) -> Option<&mut dyn Stream>;

    /// Return the next stream component
    fn next(&mut self) -> ResOpt;

    /// Callback that releases artifacts of stream
    fn on_emit_artifacts(&mut self) -> Result<Vec<Artifact>> {
        Ok(vec![])
    }

    /// Emit artifacts of stream
    ///
    /// A stream may aggregate data over time that is released by calling this method. Usually,
    /// this happens at the end of the stream.
    ///
    fn emit_artifacts(&mut self) -> Result<Vec<Artifact>> {
        let mut artifacts = Vec::new();
        if let Some(inner) = self.get_inner_mut() {
            artifacts.extend(Stream::emit_artifacts(inner)?);
        }
        artifacts.extend(Stream::on_emit_artifacts(self)?);
        Ok(artifacts)
    }
}

impl Stream for Box<dyn Stream> {
    fn get_inner(&self) -> Option<&dyn Stream> {
        self.as_ref().get_inner()
    }

    fn get_inner_mut(&mut self) -> Option<&mut dyn Stream> {
        self.as_mut().get_inner_mut()
    }

    fn next(&mut self) -> ResOpt {
        self.as_mut().next()
    }

    fn on_emit_artifacts(&mut self) -> Result<Vec<Artifact>> {
        self.as_mut().on_emit_artifacts()
    }
}

/// Stream endpoint
///
/// A stream sink acts as an endpoint for an extensible event stream and is usually used when a
/// stream is converted into different representation. If that's not intended, the `consume`
/// function is a good shortcut. It simply discards the stream's contents.
///
pub trait StreamSink: Send {
    /// Optional callback  that is invoked when the stream is opened
    fn on_open(&mut self) -> Result<()> {
        Ok(())
    }

    /// Callback that is invoked on each stream component
    fn on_component(&mut self, component: Component) -> Result<()>;

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
    fn on_emit_artifacts(&mut self) -> Result<Vec<Artifact>> {
        Ok(vec![])
    }

    /// Invokes a stream as long as it provides new components.
    fn consume(&mut self, stream: &mut dyn Stream) -> Result<Vec<Artifact>> {
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
        Ok(Stream::emit_artifacts(stream)?
            .into_iter()
            .chain(StreamSink::on_emit_artifacts(self)?.into_iter())
            .collect())
    }
}

impl StreamSink for Box<dyn StreamSink> {
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

    fn on_emit_artifacts(&mut self) -> Result<Vec<Artifact>> {
        self.as_mut().on_emit_artifacts()
    }
}

/// A dummy sink that does nothing but consuming the given stream
pub struct Void;

impl Default for Void {
    fn default() -> Self {
        Void {}
    }
}

impl StreamSink for Void {
    fn on_component(&mut self, _: Component) -> Result<()> {
        Ok(())
    }
}

/// Creates a dummy sink and consumes the given stream
pub fn consume<T: Stream>(stream: &mut T) -> Result<Vec<Artifact>> {
    Void::default().consume(stream)
}

#[cfg(test)]
mod tests {
    use crate::dev_util::load_example;

    use super::*;

    #[test]
    pub fn test_consume() {
        let mut buffer = load_example(&["book", "L1.xes"]);

        assert_eq!(buffer.len(), 7);

        consume(&mut buffer).unwrap();

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

    impl StreamSink for TestSink {
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
