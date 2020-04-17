//! Event streams
//!
//! This module provides a protocol for event streaming. Its design is strongly inspired by the
//! [XES](http://www.xes-standard.org/) standard.
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

#[derive(Debug, Clone)]
pub enum Element {
    Extension(Extension),
    Global(Global),
    Classifier(Classifier),
    Attribute(Attribute),
    Trace(Trace),
    Event(Event),
}

pub type ResOpt = Result<Option<Element>>;

pub trait Stream {
    fn next_element(&mut self) -> ResOpt;
}

pub trait StreamSink {
    fn consume<T: Stream>(&mut self, source: &mut T) -> Result<()>;
}

pub fn consume<T: Stream>(stream: &mut T) -> Result<()> {
    loop {
        match stream.next_element()? {
            None => break,
            _ => (),
        };
    }

    Ok(())
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum StreamState {
    Extension,
    Global,
    Classifier,
    Attribute,
    Trace,
    Event,
}


#[derive(Debug, Clone)]
pub struct StreamMeta {
    state: StreamState,
    extensions: HashMap<String, Extension>,
    globals: Vec<Global>,
    classifiers: HashMap<String, Classifier>,
    attributes: HashMap<String, Attribute>,
}

impl Default for StreamMeta {
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

impl StreamMeta {
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

pub trait Handler {
    fn meta(&mut self, _meta: &StreamMeta) {}
    fn trace(&mut self, trace: Trace, _meta: &StreamMeta) -> Result<Trace> {
        Ok(trace)
    }
    fn event(&mut self, event: Event, _meta: &StreamMeta) -> Result<Event> {
        Ok(event)
    }
}

#[derive(Debug, Clone)]
pub struct Observer<I: Stream, H: Handler> {
    stream: I,
    meta: StreamMeta,
    handler: Vec<H>,
}

impl<'a, I: Stream, H: Handler> Observer<I, H> {
    fn new(stream: I) -> Self {
        Observer {
            stream,
            meta: StreamMeta::default(),
            handler: Vec::new(),
        }
    }

    fn add_handler(&'a mut self, handler: H) -> &'a mut Self {
        self.handler.push(handler);
        self
    }
}

impl<I: Stream, H: Handler> Stream for Observer<I, H> {
    fn next_element(&mut self) -> ResOpt {
        if let Some(element) = self.stream.next_element()? {
            // update meta data
            let (old, new) = self.meta.update(&element)?;

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
                        trace = handler.trace(trace, &self.meta)?;
                    }

                    Element::Trace(trace)
                }
                Element::Event(event) => {
                    let mut event = event;

                    for handler in self.handler.iter_mut() {
                        event = handler.event(event, &self.meta)?;
                    }

                    Element::Event(event)
                }
            };
            Ok(Some(element))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHandler {
        meta_count: usize,
        trace_count: usize,
        event_count: usize,
    }

    impl Default for TestHandler {
        fn default() -> Self {
            Self {
                meta_count: 0,
                trace_count: 0,
                event_count: 0,
            }
        }
    }

    impl Handler for TestHandler {
        fn meta(&mut self, _meta: &StreamMeta) {
            self.meta_count += 1;
        }
        fn trace(&mut self, trace: Trace, _meta: &StreamMeta) -> Result<Trace> {
            self.trace_count += 1;
            Ok(trace)
        }
        fn event(&mut self, event: Event, _meta: &StreamMeta) -> Result<Event> {
            self.event_count += 1;
            Ok(event)
        }
    }

    impl TestHandler {
        fn counts(&self) -> (usize, usize, usize) {
            (self.meta_count, self.trace_count, self.event_count)
        }
    }

    #[test]
    fn test_consume() {
        let mut buffer = buffer::load_example(&["xes", "book", "L1.xes"]);

        assert_eq!(buffer.len(), 19);

        consume(&mut buffer).unwrap();

        assert_eq!(buffer.len(), 0);
    }
}
