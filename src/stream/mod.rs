//! Extensible event streams
//!
//! This module provides a protocol for event streaming. Its design is strongly inspired by the
//! XES standard[1] (see also [xes-standard.org](http://www.xes-standard.org)).
//!
//! Further, it comes with commonly used convenience features such as buffering, filtering, basic
//! statistics, validation and (de-)serialization. The provided API allows you to easily customize
//! your data processing pipeline.
//!
//! [1] [_IEEE Standard for eXtensible Event Stream (XES) for Achieving Interoperability in Event
//! Logs and Event Streams_, 1849:2016, 2016](https://standards.ieee.org/standard/1849-2016.html)
//!

// modules
pub mod buffer;
pub mod filter;
pub mod stats;
pub mod xes;
pub mod xesext;
pub mod xml_util;

// standard library
use std::collections::HashMap;
use std::fmt::Debug;

// third party

// local
use crate::error::{Error, Result};
use crate::{Attribute, Classifier, Event, Extension, Global, Trace};

/// Atomic unit of an extensible event stream
#[derive(Debug, Clone)]
pub enum Element {
    Extension(Extension),
    Global(Global),
    Classifier(Classifier),
    Attribute(Attribute),
    Trace(Trace),
    Event(Event),
}

/// Container for stream elements that can express the empty element as well as errors
pub type ResOpt = Result<Option<Element>>;

/// Extensible event streams
///
/// Yields one stream element at a time. Usually, it either acts as a factory or forwards another
/// stream. Errors are propagated to the caller.
///
pub trait Stream {
    /// Returns the next stream element
    fn next(&mut self) -> ResOpt;
}

/// Stream endpoint
///
/// A stream sink acts as an endpoint for an extensible event stream and is usually used when a
/// stream is converted into different representation. If that's not intended, the `consume`
/// function is a good shortcut. It simply discards the stream's contents.
///
pub trait StreamSink {
    /// Invokes a stream as long as it provides new elements.
    fn consume<T: Stream>(&mut self, source: &mut T) -> Result<()>;
}

/// Stream sink that discards consumed contents
pub fn consume<T: Stream>(stream: &mut T) -> Result<()> {
    loop {
        match stream.next()? {
            None => break,
            _ => (),
        };
    }

    Ok(())
}

/// State of an extensible event stream
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum StreamState {
    Extension,
    Global,
    Classifier,
    Attribute,
    Trace,
    Event,
}

/// Memorizes meta elements of an extensible event stream. Used by the observer
#[derive(Debug, Clone)]
pub struct MetaCache {
    state: StreamState,
    extensions: HashMap<String, Extension>,
    globals: Vec<Global>,
    classifiers: HashMap<String, Classifier>,
    attributes: HashMap<String, Attribute>,
}

impl Default for MetaCache {
    fn default() -> Self {
        Self {
            state: StreamState::Extension,
            extensions: HashMap::new(),
            globals: Vec::new(),
            classifiers: HashMap::new(),
            attributes: HashMap::new(),
        }
    }
}

impl MetaCache {
    /// Update meta cache by given element
    ///
    /// If the given element contains meta data a copy of it is cached. If the triggered state
    /// transition doesn't comply with the order obligated by the XES standard an error is thrown.
    /// Otherwise, the state transition is returned.
    ///
    fn update(&mut self, element: &Element) -> Result<(StreamState, StreamState)> {
        let old_state = self.state.clone();

        // state transition
        let new_state = match element {
            Element::Extension(e) => {
                let extension = e.clone();
                self.extensions.insert(extension.name.clone(), extension);
                StreamState::Extension
            }
            Element::Global(g) => {
                self.globals.push(g.clone());
                StreamState::Global
            }
            Element::Classifier(c) => {
                let classifier = c.clone();
                self.classifiers.insert(classifier.name.clone(), classifier);

                StreamState::Classifier
            }
            Element::Attribute(a) => {
                let attribute = a.clone();
                self.attributes.insert(attribute.key.clone(), attribute);

                StreamState::Attribute
            }
            Element::Trace(_trace) => StreamState::Trace,
            Element::Event(_event) => StreamState::Event,
        };

        // check transition
        if new_state < old_state {
            return Err(Error::StateError {
                got: new_state,
                current: old_state,
            });
        }

        // update state
        self.state = new_state.clone();

        Ok((old_state, new_state))
    }
}

/// Gets registered with an observer wile providing callbacks
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
    fn meta(&mut self, _meta: &MetaCache) {}

    /// Handle a trace
    ///
    /// Invoked on each trace that occurs in a stream. Events contained toggle a separate callback.
    ///
    fn trace(&mut self, trace: Trace, _meta: &MetaCache) -> Result<Option<Trace>> {
        Ok(Some(trace))
    }

    /// Handle an event
    ///
    /// Invoked on each event in stream. Whether the element is part of a trace is indicated by
    /// `in_trace`.
    ///
    fn event(&mut self, event: Event, _in_trace: bool, _meta: &MetaCache) -> Result<Option<Event>> {
        Ok(Some(event))
    }
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
    meta: MetaCache,
    handler: Vec<H>,
}

impl<'a, I: Stream, H: Handler> Observer<I, H> {
    /// Create new observer
    pub fn new(stream: I) -> Self {
        Observer {
            stream,
            meta: MetaCache::default(),
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

    fn handle_element(
        &mut self,
        element: Element,
        (old, new): (StreamState, StreamState),
    ) -> ResOpt {
        if old < StreamState::Trace && new >= StreamState::Trace {
            for handler in self.handler.iter_mut() {
                handler.meta(&self.meta);
            }
        }

        let element = match element {
            Element::Extension(e) => Element::Extension(e),
            Element::Global(e) => Element::Global(e),
            Element::Classifier(e) => Element::Classifier(e),
            Element::Attribute(e) => Element::Attribute(e),
            Element::Trace(trace) => {
                let mut trace = trace;

                for handler in self.handler.iter_mut() {
                    trace = match handler.trace(trace, &self.meta)? {
                        Some(trace) => trace,
                        None => return Ok(None),
                    };
                }

                let mut tmp: Vec<Event> = Vec::new();

                loop {
                    if let Some(event) = trace.events.pop() {
                        let mut event = Some(event);

                        for handler in self.handler.iter_mut() {
                            event = match event {
                                Some(event) => handler.event(event, true, &self.meta)?,
                                None => None,
                            }
                        }

                        if let Some(event) = event {
                            tmp.push(event);
                        }
                    } else {
                        break;
                    }
                }

                trace.events.append(&mut tmp);

                Element::Trace(trace)
            }
            Element::Event(event) => {
                let mut event = event;

                for handler in self.handler.iter_mut() {
                    event = match handler.event(event, false, &self.meta)? {
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
        loop {
            if let Some(element) = self.stream.next()? {
                let transition = self.meta.update(&element)?;
                if let Some(element) = self.handle_element(element, transition)? {
                    return Ok(Some(element));
                }
            } else {
                break;
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::xes::XesReader;
    use crate::util::{expand_static, open_buffered};
    use std::path::PathBuf;

    #[test]
    fn test_consume() {
        let mut buffer = buffer::load_example(&["xes", "book", "L1.xes"]);

        assert_eq!(buffer.len(), 19);

        consume(&mut buffer).unwrap();

        assert_eq!(buffer.len(), 0);
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
        fn meta(&mut self, _meta: &MetaCache) {
            self.ct_meta += 1;
        }

        fn trace(&mut self, trace: Trace, _meta: &MetaCache) -> Result<Option<Trace>> {
            self.ct_trace += 1;

            if !self.filter || self.ct_trace % 2 == 0 {
                Ok(Some(trace))
            } else {
                Ok(None)
            }
        }

        fn event(
            &mut self,
            event: Event,
            _in_trace: bool,
            _meta: &MetaCache,
        ) -> Result<Option<Event>> {
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
            ("correct", "log_correct_attributes.xes", [0, 0, 0, 0]),
            ("correct", "event_correct_attributes.xes", [1, 1, 4, 2])
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
            ("correct", "log_correct_attributes.xes", [0, 0, 0, 0]),
            ("correct", "event_correct_attributes.xes", [1, 0, 1, 0]),
        ];

        for (d, f, counts) in param.iter() {
            _test_observer(expand_static(&["xes", d, f]), counts, true)
        }
    }

    #[test]
    fn test_observer_order_validation() {
        let names = [
            "misplaced_extension_event.xes",
            "misplaced_extension_classifier.xes",
            "misplaced_trace_event.xes",
            "misplaced_global_attribute.xes",
            "misplaced_extension_trace.xes",
            "misplaced_global_event.xes",
            "misplaced_classifier_event.xes",
            "misplaced_extension_attribute.xes",
            "misplaced_attribute_event.xes",
            "misplaced_global_classifier.xes",
            "misplaced_classifier_attribute.xes",
            "misplaced_classifier_trace.xes",
            "misplaced_attribute_trace.xes",
            "misplaced_global_trace.xes",
            "misplaced_extension_global.xes",
        ];

        for n in names.iter() {
            let f = open_buffered(&expand_static(&["xes", "non_validating", n]));
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
