//! Filtering event streams.

// standard library

// third party

// local
use crate::error::Result;
use crate::stream::{
    observer::{Handler, Observer},
    Attributes, Event, Stream, Trace,
};

/// A condition aka filter function maps any item to a boolean value
pub type Condition<'a, T> = Box<dyn Fn(&T) -> Result<bool> + 'a>;

/// A vector of vectors of conditions
pub type CNF<'a, T> = Vec<Vec<Condition<'a, T>>>;

/// Filter handler for use with an observer
///
/// Contains disjunctive event and trace filters aka conditions. A trace/event is forwarded iff
/// any condition is true. Consequently, the empty condition set will always evaluate to false.
///
pub struct Filter<'a> {
    trace_filter: Vec<Condition<'a, Trace>>,
    event_filter: Vec<Condition<'a, Event>>,
}

impl<'a> Default for Filter<'a> {
    fn default() -> Self {
        Filter {
            trace_filter: Vec::new(),
            event_filter: Vec::new(),
        }
    }
}

impl<'a> Handler for Filter<'a> {
    fn on_trace(&mut self, trace: Trace) -> Result<Option<Trace>> {
        for filter in self.trace_filter.iter() {
            if filter(&trace)? {
                return Ok(Some(trace));
            }
        }

        Ok(None)
    }

    fn on_event(&mut self, event: Event, _in_trace: bool) -> Result<Option<Event>> {
        for filter in self.event_filter.iter() {
            if filter(&event)? {
                return Ok(Some(event));
            }
        }

        Ok(None)
    }
}

/// Create pseudo filter function that returns always the same value
pub fn pseudo_filter<'a, T: 'a + Attributes>(value: bool) -> Condition<'a, T> {
    Box::new(move |_x: &T| Ok(value))
}

/// Create a filter function that inverts the given filter function
pub fn neg<'a, T: 'a + Attributes>(function: Condition<'a, T>) -> Condition<'a, T> {
    Box::new(move |x: &T| Ok(!function(x)?))
}

/// Create an observer based filter from filter functions given in conjunctive normal form
///
/// Creates an instance of observer and populate it with filter handlers. The filter conditions are
/// provided in a [CNF](https://en.wikipedia.org/wiki/Conjunctive_normal_form) like fashion -- as
/// conjunctions of disjunctions.
///
pub fn from_cnf<'a, T: Stream>(
    stream: T,
    trace_filters: CNF<'a, Trace>,
    event_filters: CNF<'a, Event>,
) -> Observer<T, Filter<'a>> {
    let mut observer = Observer::new(stream);

    for conjunction in trace_filters {
        let mut filter = Filter::default();

        for disjunction in conjunction {
            filter.trace_filter.push(Box::new(disjunction));
        }

        // As the empty clause evaluates to false, we inject a pseudo filter function that always
        // returns true.
        filter.event_filter.push(pseudo_filter(true));

        observer.register(filter);
    }

    for conjunction in event_filters {
        let mut filter = Filter::default();

        for disjunction in conjunction {
            filter.event_filter.push(Box::new(disjunction));
        }

        // As the empty clause evaluates to false, we inject a pseudo filter function that always
        // returns true.
        filter.trace_filter.push(pseudo_filter(true));

        observer.register(filter);
    }

    observer
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::stream::buffer::Buffer;
    use crate::stream::Element;
    use crate::stream::StreamSink;

    pub struct Sequence {
        sequence: Vec<String>,
    }

    impl Default for Sequence {
        fn default() -> Self {
            Sequence {
                sequence: Vec::new(),
            }
        }
    }

    impl StreamSink for Sequence {
        fn on_element(&mut self, element: Element) -> Result<()> {
            match element {
                Element::Trace(trace) => {
                    self.sequence.push(String::from("["));
                    for event in trace.events.iter() {
                        self.sequence.push(match event.get("concept:name") {
                            Some(name) => name.try_string()?.to_string(),
                            None => "?".to_string(),
                        })
                    }
                    self.sequence.push(String::from("]"));
                }
                Element::Event(event) => self.sequence.push(match event.get("concept:name") {
                    Some(name) => name.try_string()?.to_string(),
                    None => "?".to_string(),
                }),
                _ => (),
            }

            Ok(())
        }
    }

    impl Sequence {
        pub fn as_string(&self) -> String {
            self.sequence.join("")
        }
    }

    /// Convenience function for testing filters
    pub fn test_filter(
        buffer: Buffer,
        trace_filter: CNF<Trace>,
        event_filter: CNF<Event>,
        sequence: &str,
    ) {
        let mut filter = from_cnf(buffer, trace_filter, event_filter);

        let mut result = Sequence::default();
        result.consume(&mut filter).unwrap();

        assert_eq!(sequence, result.as_string());
    }
}
