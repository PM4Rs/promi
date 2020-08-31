//! Filtering event streams.

// standard library

// third party

// local
use crate::error::Result;
use crate::stream::{
    observer::{Handler, Observer},
    Attributes, Event, Meta, Stream, Trace,
};

/// A condition aka filter function maps any item to a boolean value
pub type Condition<'a, T> = Box<dyn Fn(&T) -> Result<bool> + 'a>;

/// Filter handler for use with an observer
///
/// Contains disjunctive event and trace filters aka conditions. A trace/event is forwarded iff
/// any condition is true. Consequently, the empty condition set will always evaluate to false.
///
pub struct Filter<'a> {
    event_filter: Vec<Condition<'a, Event>>,
    trace_filter: Vec<Condition<'a, Trace>>,
}

impl<'a> Default for Filter<'a> {
    fn default() -> Self {
        Filter {
            event_filter: Vec::new(),
            trace_filter: Vec::new(),
        }
    }
}

impl<'a> Handler for Filter<'a> {
    fn on_trace(&mut self, trace: Trace, _meta: &Meta) -> Result<Option<Trace>> {
        for filter in self.trace_filter.iter() {
            if filter(&trace)? {
                return Ok(Some(trace));
            }
        }

        Ok(None)
    }

    fn on_event(&mut self, event: Event, _in_trace: bool, _meta: &Meta) -> Result<Option<Event>> {
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
pub fn from_cnfs<'a, T: Stream>(
    stream: T,
    event_filters: Vec<Vec<Condition<'a, Event>>>,
    trace_filters: Vec<Vec<Condition<'a, Trace>>>,
) -> Observer<T, Filter<'a>> {
    let mut observer = Observer::new(stream);

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

    observer
}
