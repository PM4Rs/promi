//! Collecting live statistics from an event stream.
//!

// standard library
use std::fmt;
use std::fmt::{Debug, Formatter};

// third party

// local
use crate::error::Result;
use crate::stream::{Element, Handler, ResOpt, Stream};
use crate::{Event, Meta, Trace};

/// Count element types in an extensible event stream
#[derive(Debug)]
pub struct Counter<T: Stream> {
    stream: T,
    pub meta: usize,
    pub traces: usize,
    pub events: usize,
}

impl<T: Stream> Counter<T> {
    /// New counter from stream
    pub fn new(stream: T) -> Self {
        Counter {
            stream,
            meta: 0,
            traces: 0,
            events: 0,
        }
    }

    /// Counts as array
    pub fn counts(&self) -> [usize; 3] {
        [self.meta, self.traces, self.events]
    }
}

impl<T: Stream> Stream for Counter<T> {
    fn next(&mut self) -> ResOpt {
        let element = self.stream.next()?;

        match &element {
            Some(Element::Meta(_)) => self.meta += 1,
            Some(Element::Trace(_)) => self.traces += 1,
            Some(Element::Event(_)) => self.events += 1,
            None => (),
        }

        Ok(element)
    }
}

impl<T: Stream> fmt::Display for Counter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Counts")?;
        writeln!(f, "   meta:   {}", self.meta)?;
        writeln!(f, "   traces: {}", self.traces)?;
        writeln!(f, "   events: {}", self.events)?;
        Ok(())
    }
}

/// Event and trace statistics
///
/// Provides deeper inspection of an extensible event stream by looking into traces and providing
/// aggregated statistics.
///
#[derive(Debug)]
pub struct StreamStats {
    ct_trace: Vec<usize>,
    ct_event: usize,
}

impl Default for StreamStats {
    fn default() -> Self {
        Self {
            ct_trace: Vec::new(),
            ct_event: 0,
        }
    }
}

impl Handler for StreamStats {
    fn trace(&mut self, trace: Trace, _meta: &Meta) -> Result<Option<Trace>> {
        self.ct_trace.push(trace.events.len());
        Ok(Some(trace))
    }

    fn event(&mut self, event: Event, _in_trace: bool, _meta: &Meta) -> Result<Option<Event>> {
        self.ct_event += 1;
        Ok(Some(event))
    }
}

impl StreamStats {
    /// Counts as array
    pub fn counts(&self) -> [usize; 2] {
        [self.ct_trace.len(), self.ct_event]
    }
}

impl fmt::Display for StreamStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let sa_events = self.ct_event - self.ct_trace.iter().sum::<usize>();
        writeln!(f, "StreamStats")?;
        writeln!(f, "   traces:              {:?}", self.ct_trace.len())?;
        writeln!(f, "   events:              {:?}", self.ct_event)?;
        writeln!(f, "   events (standalone): {:?}", sa_events)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream;
    use crate::stream::buffer::tests::load_example;

    #[test]
    fn test_counter() {
        let param = [
            ("book", "L1.xes", [1, 6, 0]),
            ("book", "L2.xes", [1, 13, 0]),
            ("book", "L3.xes", [1, 4, 0]),
            ("book", "L4.xes", [1, 147, 0]),
            ("book", "L5.xes", [1, 14, 0]),
            ("correct", "log_correct_attributes.xes", [1, 0, 0]),
            ("correct", "event_correct_attributes.xes", [1, 1, 2]),
        ];

        for (d, f, e) in param.iter() {
            let mut stats = Counter::new(load_example(&["xes", d, f]));

            stream::consume(&mut stats).unwrap();

            assert_eq!(stats.counts(), *e);
        }
    }

    #[test]
    fn test_stream_stats() {
        let param = [
            ("book", "L1.xes", [6, 23]),
            ("book", "L2.xes", [13, 80]),
            ("book", "L3.xes", [4, 39]),
            ("book", "L4.xes", [147, 441]),
            ("book", "L5.xes", [14, 92]),
            ("correct", "log_correct_attributes.xes", [0, 0]),
            ("correct", "event_correct_attributes.xes", [1, 4]),
        ];

        for (d, f, e) in param.iter() {
            let buffer = load_example(&["xes", d, f]);
            let mut observer = stream::Observer::new(buffer);
            observer.register(StreamStats::default());

            stream::consume(&mut observer).unwrap();

            let stats = observer.release().unwrap();
            assert_eq!(stats.counts(), *e);
        }
    }
}
