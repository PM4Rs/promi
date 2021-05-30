//! Filtering event streams.

use crate::error::Result;
use crate::stream::{
    observer::{Handler, Observer},
    Attributes, Event, Stream, Trace,
};

/// A condition aka filter function maps any item to a boolean value
pub type Condition<'a, T> = Box<dyn Fn(&T) -> Result<bool> + 'a + Send>;

/// A vector of vectors of conditions
#[allow(clippy::upper_case_acronyms)]
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

/// Create a filter function that inverts the given filter function
pub fn drop_err<'a, T: 'a + Attributes>(function: Condition<'a, T>) -> Condition<'a, T> {
    Box::new(move |x: &T| Ok(function(x).unwrap_or(false)))
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
    use crate::stream::buffer::Buffer;
    use crate::stream::Component;
    use crate::stream::Sink;

    use super::*;

    pub type TokenMapper = Box<dyn Fn(&dyn Attributes) -> Result<String> + Send>;

    pub struct Sequencer {
        token_mapper: TokenMapper,
        tokens: Vec<String>,
    }

    impl Sequencer {
        pub fn new(token_mapper: TokenMapper) -> Self {
            Sequencer {
                token_mapper,
                tokens: Vec::new(),
            }
        }

        pub fn as_string(&self) -> String {
            self.tokens.join("")
        }
    }

    impl Default for Sequencer {
        /// The default Sequencer uses the `concept:name` attribute as token
        fn default() -> Self {
            Self::new(Box::new(|component| {
                Ok(match component.get("concept:name") {
                    Some(name) => name.try_string()?,
                    None => "?",
                }
                .to_string())
            }))
        }
    }

    impl Sink for Sequencer {
        fn on_component(&mut self, component: Component) -> Result<()> {
            match component {
                Component::Trace(trace) => {
                    self.tokens.push(String::from("["));
                    for event in trace.events.iter() {
                        self.tokens.push((self.token_mapper)(event)?)
                    }
                    self.tokens.push(String::from("]"));
                }
                Component::Event(event) => self.tokens.push((self.token_mapper)(&event)?),
                _ => (),
            }

            Ok(())
        }
    }

    /// Convenience function for testing filters
    pub fn test_filter(
        buffer: Buffer,
        trace_filter: CNF<Trace>,
        event_filter: CNF<Event>,
        sequence: &str,
        token_mapper: Option<TokenMapper>,
    ) {
        let mut filter = from_cnf(buffer, trace_filter, event_filter);
        let mut result = match token_mapper {
            Some(mapper) => Sequencer::new(mapper),
            None => Sequencer::default(),
        };

        result.consume(&mut filter).unwrap();

        assert_eq!(sequence, result.as_string());
    }
}
