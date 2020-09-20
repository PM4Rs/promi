//! A stateful observer that allows for registering callbacks to handle stream elements

// standard library

// third party

// local
use crate::error::{Error, Result};
use crate::stream::{Element, ElementType, Event, Meta, ResOpt, Stream, Trace, WrappingStream};

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
    fn on_meta(&mut self, meta: Meta) -> Result<Meta> {
        Ok(meta)
    }

    /// Handle a trace
    ///
    /// Invoked on each trace that occurs in a stream. Events contained toggle a separate callback.
    ///
    fn on_trace(&mut self, trace: Trace) -> Result<Option<Trace>> {
        Ok(Some(trace))
    }

    /// Handle an event
    ///
    /// Invoked on each event in stream. Whether the element is part of a trace is indicated by
    /// `in_trace`.
    ///
    fn on_event(&mut self, event: Event, _in_trace: bool) -> Result<Option<Event>> {
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
    state: ElementType,
    handler: Vec<H>,
}

impl<'a, I: Stream, H: Handler> Observer<I, H> {
    /// Create new observer
    pub fn new(stream: I) -> Self {
        Observer {
            stream,
            state: ElementType::Meta,
            handler: Vec::new(),
        }
    }

    /// Register a new handler
    pub fn register(&'a mut self, handler: H) {
        self.handler.push(handler)
    }

    /// Release handler (reverse registering order)
    pub fn release(&mut self) -> Option<H> {
        self.handler.pop()
    }

    fn update_state(&mut self, state: ElementType) -> Result<()> {
        if self.state > state {
            Err(Error::StateError(format!(
                "invalid transition: {:?} --> {:?}",
                self.state, state
            )))
        } else {
            self.state = state;
            Ok(())
        }
    }

    fn on_element(&mut self, element: Element) -> ResOpt {
        let element = match element {
            Element::Meta(meta) => {
                // Since there's only one meta element allowed, we can directly jump to trace state
                self.update_state(ElementType::Trace)?;

                // call all the handlers
                let mut meta = meta;
                for handler in self.handler.iter_mut() {
                    meta = handler.on_meta(meta)?;
                }

                Element::Meta(meta)
            }
            Element::Trace(trace) => {
                self.update_state(ElementType::Trace)?;

                // apply all handlers on trace
                let mut trace = trace;
                for handler in self.handler.iter_mut() {
                    trace = match handler.on_trace(trace)? {
                        Some(trace) => trace,
                        None => return Ok(None),
                    };
                }

                // apply all handlers on events within trace
                let mut events: Vec<Event> = Vec::new();
                for event in trace.events.drain(..) {
                    let mut event = Some(event);

                    for handler in self.handler.iter_mut() {
                        event = match event {
                            Some(event) => handler.on_event(event, true)?,
                            None => None,
                        }
                    }

                    if let Some(event) = event {
                        events.push(event);
                    }
                }

                trace.events = events;
                Element::Trace(trace)
            }
            Element::Event(event) => {
                self.update_state(ElementType::Event)?;

                // apply all handlers on the event
                let mut event = event;
                for handler in self.handler.iter_mut() {
                    event = match handler.on_event(event, false)? {
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

impl<I: Stream, H: Handler> From<(I, Vec<H>)> for Observer<I, H> {
    fn from(components: (I, Vec<H>)) -> Self {
        let (stream, handlers) = components;
        let mut observer = Observer::new(stream);

        for handler in handlers {
            observer.register(handler)
        }

        observer
    }
}

impl<I: Stream, H: Handler> From<(I, H)> for Observer<I, H> {
    fn from(components: (I, H)) -> Self {
        let (stream, handler) = components;
        let mut observer = Observer::new(stream);

        observer.register(handler);

        observer
    }
}

impl<I: Stream, H: Handler> Stream for Observer<I, H> {
    fn next(&mut self) -> ResOpt {
        while let Some(element) = self.stream.next()? {
            if let Some(element) = self.on_element(element)? {
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
    use crate::stream::{consume, xes::XesReader};
    use std::path::PathBuf;

    #[derive(Debug)]
    struct TestHandler {
        filter: bool,
        ct_meta: usize,
        ct_trace: usize,
        ct_event: usize,
        ct_in_trace: usize,
    }

    impl Handler for TestHandler {
        fn on_meta(&mut self, meta: Meta) -> Result<Meta> {
            self.ct_meta += 1;
            Ok(meta)
        }

        fn on_trace(&mut self, trace: Trace) -> Result<Option<Trace>> {
            self.ct_trace += 1;

            if !self.filter || self.ct_trace % 2 == 0 {
                Ok(Some(trace))
            } else {
                Ok(None)
            }
        }

        fn on_event(&mut self, event: Event, _in_trace: bool) -> Result<Option<Event>> {
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