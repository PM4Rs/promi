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

// modules
pub mod buffer;
pub mod channel;
pub mod duplicator;
pub mod observer;
pub mod split;
pub mod stats;
pub mod xes;
pub mod xml_util;

// standard library
use std::collections::BTreeMap; // TODO consider going back to HashMaps
use std::convert::TryFrom;
use std::fmt::Debug;

// third party

// local
use crate::{DateTime, Error, Result};

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

// TODO testing
impl AttributeValue {
    pub fn try_string(&self) -> Result<&String> {
        match self {
            AttributeValue::String(string) => Ok(string),
            other => Err(Error::AttributeError(format!("{:?} is no string", other))),
        }
    }

    pub fn try_date(&self) -> Result<&DateTime> {
        match self {
            AttributeValue::Date(timestamp) => Ok(timestamp),
            other => Err(Error::AttributeError(format!("{:?} is no datetime", other))),
        }
    }

    pub fn try_int(&self) -> Result<&i64> {
        match self {
            AttributeValue::Int(integer) => Ok(integer),
            other => Err(Error::AttributeError(format!("{:?} is no integer", other))),
        }
    }

    pub fn try_float(&self) -> Result<&f64> {
        match self {
            AttributeValue::Float(float) => Ok(float),
            other => Err(Error::AttributeError(format!("{:?} is no float", other))),
        }
    }

    pub fn try_boolean(&self) -> Result<&bool> {
        match self {
            AttributeValue::Boolean(boolean) => Ok(boolean),
            other => Err(Error::AttributeError(format!("{:?} is no integer", other))),
        }
    }

    pub fn try_id(&self) -> Result<&String> {
        match self {
            AttributeValue::Id(id) => Ok(id),
            other => Err(Error::AttributeError(format!("{:?} is no id", other))),
        }
    }

    pub fn try_list(&self) -> Result<&[Attribute]> {
        match self {
            AttributeValue::List(list) => Ok(list),
            other => Err(Error::AttributeError(format!("{:?} is no list", other))),
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

/// Provide semantics for sets of attributes
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > An extension defines a (possibly empty) set of attributes for every type of component.
/// > The extension provides points of reference for interpreting these attributes, and, thus, their
/// > components. Extensions, therefore, are primarily a vehicle for attaching semantics to a set of
/// > defined attributes per component.
///
#[derive(Debug, Clone)]
pub struct Extension {
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

/// Assigns an identity to trace or event
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > A classifier assigns an identity to each event that
/// > makes it comparable to others (via their assigned identity). Examples of such identities
/// > include the descriptive name of the event, the descriptive name of the case the event
/// > relates to, the descriptive name of the cause of the event, and the descriptive name of the
/// > case related to the event.
///
#[derive(Debug, Clone)]
pub struct Classifier {
    name: String,
    scope: Scope,
    keys: String,
}

/// Holds meta information of an extensible event stream
#[derive(Debug, Clone)]
pub struct Meta {
    extensions: Vec<Extension>,
    globals: Vec<Global>,
    classifiers: Vec<Classifier>,
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

        buffer.push(Ok(Some(Element::Meta(self.meta))));

        for trace in self.traces {
            buffer.push(Ok(Some(Element::Trace(trace))));
        }

        for event in self.events {
            buffer.push(Ok(Some(Element::Event(event))));
        }

        buffer
    }
}

impl StreamSink for Log {
    fn on_element(&mut self, element: Element) -> Result<()> {
        match element {
            Element::Meta(meta) => self.meta = meta,
            Element::Trace(trace) => self.traces.push(trace),
            Element::Event(event) => self.events.push(event),
        };

        Ok(())
    }
}

/// Atomic unit of an extensible event stream
#[derive(Debug, Clone)]
pub enum Element {
    Meta(Meta),
    Trace(Trace),
    Event(Event),
}

/// Container for stream elements that can express the empty element as well as errors
pub type ResOpt = Result<Option<Element>>;

/// Extensible event stream
///
/// Yields one stream element at a time. Usually, it either acts as a factory or forwards another
/// stream. Errors are propagated to the caller.
///
pub trait Stream {
    /// Returns the next stream element
    fn next(&mut self) -> ResOpt;
}

/// Extensible event stream that wraps another stream instance
///
/// Usually, one wants to chain stream instances. TODO finish docs
///
pub trait WrappingStream<T: Stream>: Stream {
    /// Get a reference to inner stream
    fn inner(&self) -> &T;

    /// Release inner stream
    fn into_inner(self) -> T;
}

/// Stream endpoint
///
/// A stream sink acts as an endpoint for an extensible event stream and is usually used when a
/// stream is converted into different representation. If that's not intended, the `consume`
/// function is a good shortcut. It simply discards the stream's contents.
///
pub trait StreamSink {
    /// Optional callback  that is invoked when the stream is opened
    fn on_open(&mut self) -> Result<()> {
        Ok(())
    }

    /// Callback that is invoked on each stream element
    fn on_element(&mut self, element: Element) -> Result<()>;

    /// Optional callback that is invoked once the stream is closed
    fn on_close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Optional callback that is invoked when an error occurs
    fn on_error(&mut self, _error: Error) -> Result<()> {
        Ok(())
    }

    /// Invokes a stream as long as it provides new elements.
    fn consume<T: Stream>(&mut self, stream: &mut T) -> Result<()> {
        self.on_open()?;

        loop {
            match stream.next() {
                Ok(Some(element)) => self.on_element(element)?,
                Ok(None) => break,
                Err(error) => {
                    self.on_error(error.clone())?;
                    return Err(error);
                }
            };
        }

        self.on_close()?;
        Ok(())
    }
}

/// Stream sink that discards consumed contents
pub fn consume<T: Stream>(stream: &mut T) -> Result<()> {
    while stream.next()?.is_some() { /* discard stream contents */ }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_consume() {
        let mut buffer = buffer::tests::load_example(&["xes", "book", "L1.xes"]);

        assert_eq!(buffer.len(), 7);

        consume(&mut buffer).unwrap();

        assert_eq!(buffer.len(), 0);
    }

    #[derive(Debug)]
    pub struct TestSink {
        ct_open: usize,
        ct_element: usize,
        ct_close: usize,
        ct_error: usize,
    }

    impl Default for TestSink {
        fn default() -> Self {
            TestSink {
                ct_open: 0,
                ct_element: 0,
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

        fn on_element(&mut self, _: Element) -> Result<()> {
            self.ct_element += 1;
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
            [self.ct_open, self.ct_element, self.ct_close, self.ct_error]
        }
    }
}
