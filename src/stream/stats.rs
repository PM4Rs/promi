//! Collecting live statistics from an event stream.
//!

// standard library
use std::fmt;
use std::fmt::Debug;

// third party

// local
use crate::stream::{Element, ResOpt, Stream};

#[derive(Debug)]
pub struct StreamStats<T: Stream> {
    stream: T,
    pub extensions: usize,
    pub globals: usize,
    pub classifiers: usize,
    pub attributes: usize,
    pub traces: usize,
    pub events: usize,
}

impl<T: Stream> StreamStats<T> {
    pub fn new(stream: T) -> Self {
        StreamStats {
            stream,
            extensions: 0,
            globals: 0,
            classifiers: 0,
            attributes: 0,
            traces: 0,
            events: 0,
        }
    }
}

impl<T: Stream> Stream for StreamStats<T> {
    fn next_element(&mut self) -> ResOpt {
        let element = self.stream.next_element()?;

        match &element {
            Some(Element::Extension(_)) => self.extensions += 1,
            Some(Element::Global(_)) => self.globals += 1,
            Some(Element::Classifier(_)) => self.classifiers += 1,
            Some(Element::Attribute(_)) => self.attributes += 1,
            Some(Element::Trace(_)) => self.traces += 1,
            Some(Element::Event(_)) => self.events += 1,
            None => (),
        }

        Ok(element)
    }
}

impl<T: Stream> fmt::Display for StreamStats<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "StreamStats")?;
        writeln!(f, "   extensions:  {}", self.extensions)?;
        writeln!(f, "   globals:     {}", self.globals)?;
        writeln!(f, "   classifiers: {}", self.classifiers)?;
        writeln!(f, "   attributes:  {}", self.attributes)?;
        writeln!(f, "   traces:      {}", self.traces)?;
        writeln!(f, "   events:      {}", self.events)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream;
    use crate::stream::buffer;

    fn counts<T: Stream>(stats: &StreamStats<T>) -> [usize; 6] {
        [
            stats.extensions,
            stats.globals,
            stats.classifiers,
            stats.attributes,
            stats.traces,
            stats.events,
        ]
    }

    #[test]
    fn test_stats() {
        let foo = [
            ([5, 2, 3, 3, 6, 0], "L1.xes"),
            ([5, 2, 3, 3, 13, 0], "L2.xes"),
            ([5, 2, 3, 3, 4, 0], "L3.xes"),
            ([5, 2, 3, 3, 147, 0], "L4.xes"),
            ([5, 2, 3, 3, 14, 0], "L5.xes"),
            ([5, 2, 3, 3, 50, 0], "L11.xes"),
            ([5, 2, 3, 3, 200, 0], "L12.xes"),
        ];

        for (e, f) in foo.iter() {
            let mut stats = StreamStats::new(buffer::load_example(&["xes", "book", f]));
            stream::consume(&mut stats).unwrap();
            assert_eq!(*e, counts(&stats));
        }
    }
}
