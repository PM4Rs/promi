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
pub mod filter;
pub mod split;
pub mod stats;
pub mod xes;
pub mod xml_util;

// standard library
use std::collections::BTreeMap;
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
#[derive(Debug, Clone)]
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
    while let Some(_) = stream.next()? { /* discard stream contents */ }
    Ok(())
}

/// Creates a copy of an extensible event stream on the fly
///
/// A duplicator forwards a stream while copying each element (and errors) to forward them to the
/// given stream sink.
///
pub struct Duplicator<T: Stream, S: StreamSink> {
    stream: T,
    sink: S,
    open: bool,
}

impl<T: Stream, S: StreamSink> Duplicator<T, S> {
    /// Create a new duplicator
    pub fn new(stream: T, sink: S) -> Self {
        Duplicator {
            stream,
            sink,
            open: false,
        }
    }

    /// Drop duplicator and release sink
    pub fn into_sink(self) -> S {
        self.sink
    }
}

impl<T: Stream, S: StreamSink> Stream for Duplicator<T, S> {
    fn next(&mut self) -> ResOpt {
        if !self.open {
            self.open = true;
            self.sink.on_open()?;
        }

        match self.stream.next() {
            Ok(Some(element)) => {
                self.sink.on_element(element.clone())?;
                Ok(Some(element))
            }
            Ok(None) => {
                self.sink.on_close()?;
                Ok(None)
            }
            Err(error) => {
                self.sink.on_error(error.clone())?;
                Err(error)
            }
        }
    }
}

impl<T: Stream, S: StreamSink> WrappingStream<T> for Duplicator<T, S> {
    fn inner(&self) -> &T {
        &self.stream
    }

    fn into_inner(self) -> T {
        self.stream
    }
}

/// Gets registered with an observer while providing callbacks
///
/// All callback functions are optional. The `meta` callback is revoked once a transition from meta
/// data to payload is passed. `trace` is revoked on all traces, `event` on all events regardless of
/// whether or not it's part of a trace. Payload callbacks may also act as a filter and not return
/// the element.
///
pub trait Handler {
    /// Handle stream meta data
    ///
    /// Invoked once per stream when transition from meta data to payload is passed.
    ///
    fn meta(&mut self, _meta: &Meta) {}

    /// Handle a trace
    ///
    /// Invoked on each trace that occurs in a stream. Events contained toggle a separate callback.
    ///
    fn trace(&mut self, trace: Trace, _meta: &Meta) -> Result<Option<Trace>> {
        Ok(Some(trace))
    }

    /// Handle an event
    ///
    /// Invoked on each event in stream. Whether the element is part of a trace is indicated by
    /// `in_trace`.
    ///
    fn event(&mut self, event: Event, _in_trace: bool, _meta: &Meta) -> Result<Option<Event>> {
        Ok(Some(event))
    }
}

/// State of an extensible event stream
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum StreamState {
    Meta,
    Trace,
    Event,
}

/// Observes a stream and revokes registered callbacks
///
/// An observer preserves a state with copies of meta data elements. It manages an arbitrary number
/// of registered handlers and invokes their callbacks. Further, it checks if elements of the stream
/// occur in a valid order.
///
#[derive(Debug, Clone)]
pub struct Observer<I: Stream, H: Handler> {
    stream: I,
    state: StreamState,
    meta: Option<Meta>,
    handler: Vec<H>,
}

impl<'a, I: Stream, H: Handler> Observer<I, H> {
    /// Create new observer
    pub fn new(stream: I) -> Self {
        Observer {
            stream,
            state: StreamState::Meta,
            meta: None,
            handler: Vec::new(),
        }
    }

    /// Register a new handler
    pub fn register(&'a mut self, handler: H) -> &'a mut Self {
        self.handler.push(handler);
        self
    }

    /// Release handler (opposite registering order)
    pub fn release(&mut self) -> Option<H> {
        self.handler.pop()
    }

    fn update_state(&mut self, state: StreamState) -> Result<()> {
        if self.state <= state {
            self.state = state;
            return Ok(());
        }

        Err(Error::StateError(format!(
            "invalid transition: {:?} --> {:?}",
            self.state, state
        )))
    }

    fn handle_element(&mut self, element: Element) -> ResOpt {
        let element = match element {
            Element::Meta(meta) => {
                self.update_state(StreamState::Meta)?;
                for handler in self.handler.iter_mut() {
                    handler.meta(&meta);
                }
                self.meta = Some(meta.clone());
                Element::Meta(meta)
            }
            Element::Trace(trace) => {
                self.update_state(StreamState::Trace)?;

                let mut trace = trace;
                let meta = self
                    .meta
                    .as_ref()
                    // TODO meta element is missing error
                    .ok_or_else(|| Error::StateError("TODO proper error".to_string()))?;

                for handler in self.handler.iter_mut() {
                    trace = match handler.trace(trace, meta)? {
                        Some(trace) => trace,
                        None => return Ok(None),
                    };
                }

                let mut tmp: Vec<Event> = Vec::new();

                while let Some(event) = trace.events.pop() {
                    let mut event = Some(event);

                    for handler in self.handler.iter_mut() {
                        event = match event {
                            Some(event) => handler.event(event, true, meta)?,
                            None => None,
                        }
                    }

                    if let Some(event) = event {
                        tmp.push(event);
                    }
                }

                trace.events.append(&mut tmp);

                Element::Trace(trace)
            }
            Element::Event(event) => {
                self.update_state(StreamState::Event)?;

                let mut event = event;
                let meta = self
                    .meta
                    .as_ref()
                    // TODO meta element is missing error
                    .ok_or_else(|| Error::StateError("TODO proper error".to_string()))?;

                for handler in self.handler.iter_mut() {
                    event = match handler.event(event, false, meta)? {
                        Some(event) => event,
                        None => return Ok(None),
                    };
                }

                Element::Event(event)
            }
        };

        Ok(Some(element))
    }
}

impl<I: Stream, H: Handler> Stream for Observer<I, H> {
    fn next(&mut self) -> ResOpt {
        while let Some(element) = self.stream.next()? {
            if let Some(element) = self.handle_element(element)? {
                return Ok(Some(element));
            }
        }

        Ok(None)
    }
}

impl<I: Stream, H: Handler> WrappingStream<I> for Observer<I, H> {
    fn inner(&self) -> &I {
        &self.stream
    }

    fn into_inner(self) -> I {
        self.stream
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_util::{expand_static, open_buffered};
    use crate::stream::xes::XesReader;
    use std::path::PathBuf;

    #[test]
    fn test_consume() {
        let mut buffer = buffer::tests::load_example(&["xes", "book", "L1.xes"]);

        assert_eq!(buffer.len(), 7);

        consume(&mut buffer).unwrap();

        assert_eq!(buffer.len(), 0);
    }

    #[derive(Debug)]
    struct TestSink {
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
        fn counts(&self) -> [usize; 4] {
            [self.ct_open, self.ct_element, self.ct_close, self.ct_error]
        }
    }

    fn _test_sink_duplicator(path: PathBuf, counts: &[usize; 4], expect_error: bool) {
        let f = open_buffered(&path);
        let reader = XesReader::from(f);
        let sink_1 = TestSink::default();
        let mut sink_2 = TestSink::default();
        let mut duplicator = Duplicator::new(reader, sink_1);

        assert_eq!(sink_2.consume(&mut duplicator).is_err(), expect_error);

        let sink_1 = duplicator.into_sink();

        assert_eq!(&sink_1.counts(), counts);
        assert_eq!(&sink_2.counts(), counts);
    }

    #[test]
    fn test_sink_duplicator() {
        let param = [
            // open element close error
            ("book", "L1.xes", [1, 7, 1, 0]),
            ("book", "L2.xes", [1, 14, 1, 0]),
            ("book", "L3.xes", [1, 5, 1, 0]),
            ("book", "L4.xes", [1, 148, 1, 0]),
            ("book", "L5.xes", [1, 15, 1, 0]),
            ("correct", "log_correct_attributes.xes", [1, 1, 1, 0]),
            ("correct", "event_correct_attributes.xes", [1, 4, 1, 0]),
        ];

        for (d, f, counts) in param.iter() {
            _test_sink_duplicator(expand_static(&["xes", d, f]), counts, false);
        }

        let param = [
            ("non_parsing", "boolean_incorrect_value.xes", [1, 0, 0, 1]),
            ("non_parsing", "broken_xml.xes", [1, 6, 0, 1]),
            ("non_parsing", "element_incorrect.xes", [1, 0, 0, 1]),
            ("non_parsing", "no_log.xes", [1, 0, 0, 1]),
            ("non_parsing", "global_incorrect_scope.xes", [1, 0, 0, 1]),
        ];

        for (d, f, counts) in param.iter() {
            _test_sink_duplicator(expand_static(&["xes", d, f]), counts, true);
        }
    }

    #[derive(Debug)]
    struct TestHandler {
        filter: bool,
        ct_meta: usize,
        ct_trace: usize,
        ct_event: usize,
        ct_in_trace: usize,
    }

    impl Handler for TestHandler {
        fn meta(&mut self, _meta: &Meta) {
            self.ct_meta += 1;
        }

        fn trace(&mut self, trace: Trace, _meta: &Meta) -> Result<Option<Trace>> {
            self.ct_trace += 1;

            if !self.filter || self.ct_trace % 2 == 0 {
                Ok(Some(trace))
            } else {
                Ok(None)
            }
        }

        fn event(&mut self, event: Event, _in_trace: bool, _meta: &Meta) -> Result<Option<Event>> {
            self.ct_event += 1;

            if _in_trace {
                self.ct_in_trace += 1;
            }

            if !self.filter || self.ct_event % 2 == 0 {
                Ok(Some(event))
            } else {
                Ok(None)
            }
        }
    }

    impl TestHandler {
        fn new(filter: bool) -> Self {
            Self {
                filter,
                ct_meta: 0,
                ct_trace: 0,
                ct_event: 0,
                ct_in_trace: 0,
            }
        }

        fn counts(&self) -> [usize; 4] {
            [self.ct_meta, self.ct_trace, self.ct_event, self.ct_in_trace]
        }
    }

    fn _test_observer(path: PathBuf, counts: &[usize; 4], filter: bool) {
        let f = open_buffered(&path);
        let reader = XesReader::from(f);
        let mut observer = Observer::new(reader);

        observer.register(TestHandler::new(filter));
        observer.register(TestHandler::new(false));

        consume(&mut observer).unwrap();

        let handler_2 = observer.release().unwrap();
        let handler_1 = observer.release().unwrap();

        if !filter {
            assert_eq!(&handler_1.counts(), counts);
        }
        assert_eq!(&handler_2.counts(), counts);
    }

    #[test]
    fn test_observer_handling() {
        let param = [
            ("book", "L1.xes", [1, 6, 23, 23]),
            ("book", "L2.xes", [1, 13, 80, 80]),
            ("book", "L3.xes", [1, 4, 39, 39]),
            ("book", "L4.xes", [1, 147, 441, 441]),
            ("book", "L5.xes", [1, 14, 92, 92]),
            ("correct", "log_correct_attributes.xes", [1, 0, 0, 0]),
            ("correct", "event_correct_attributes.xes", [1, 1, 4, 2]),
        ];

        for (d, f, counts) in param.iter() {
            _test_observer(expand_static(&["xes", d, f]), counts, false)
        }
    }

    #[test]
    fn test_observer_filtering() {
        let param = [
            ("book", "L1.xes", [1, 3, 6, 6]),
            ("book", "L2.xes", [1, 6, 18, 18]),
            ("book", "L3.xes", [1, 2, 6, 6]),
            ("book", "L4.xes", [1, 73, 109, 109]),
            ("book", "L5.xes", [1, 7, 23, 23]),
            ("correct", "log_correct_attributes.xes", [1, 0, 0, 0]),
            ("correct", "event_correct_attributes.xes", [1, 0, 1, 0]),
        ];

        for (d, f, counts) in param.iter() {
            _test_observer(expand_static(&["xes", d, f]), counts, true)
        }
    }

    #[test]
    fn test_observer_order_validation() {
        let names = [
            ("non_parsing", "misplaced_extension_event.xes"),
            ("non_parsing", "misplaced_extension_trace.xes"),
            ("non_parsing", "misplaced_global_event.xes"),
            ("non_parsing", "misplaced_classifier_event.xes"),
            ("non_parsing", "misplaced_attribute_event.xes"),
            ("non_parsing", "misplaced_classifier_trace.xes"),
            ("non_parsing", "misplaced_attribute_trace.xes"),
            ("non_parsing", "misplaced_global_trace.xes"),
            ("non_validating", "misplaced_trace_event.xes"),
        ];

        for (d, n) in names.iter() {
            let f = open_buffered(&expand_static(&["xes", d, n]));
            let reader = XesReader::from(f);
            let mut observer = Observer::new(reader);

            observer.register(TestHandler::new(false));

            assert!(
                consume(&mut observer).is_err(),
                format!("expected state error: {:?}", n)
            )
        }
    }
}
