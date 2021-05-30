//! Buffering event streams.
//!
//! A buffer is essentially a fifo queue that supports the streaming protocol and can be used as a
//! stream sink. Apart from that, a buffer is a pretty dumb data structure. If you're interested in
//! a thread safe way of buffering an event stream, have a look at channels.
//!

use std::collections::VecDeque;
use std::fmt::Debug;

use crate::error::{Error, Result};
use crate::stream::log::Log;
use crate::stream::{Component, ResOpt, Sink, Stream};

/// Consumes a stream and stores it in memory for further processing.
///
#[derive(Debug, Clone)]
pub struct Buffer {
    buffer: VecDeque<ResOpt>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            buffer: VecDeque::new(),
        }
    }
}

impl Stream for Buffer {
    fn inner_ref(&self) -> Option<&dyn Stream> {
        None
    }

    fn inner_mut(&mut self) -> Option<&mut dyn Stream> {
        None
    }

    fn next(&mut self) -> ResOpt {
        match self.buffer.pop_front() {
            Some(component) => component,
            None => Ok(None),
        }
    }
}

impl Sink for Buffer {
    fn on_component(&mut self, component: Component) -> Result<()> {
        self.buffer.push_back(Ok(Some(component)));
        Ok(())
    }

    fn on_error(&mut self, error: Error) -> Result<()> {
        self.buffer.push_back(Err(error));
        Ok(())
    }
}

impl From<Log> for Buffer {
    fn from(log: Log) -> Self {
        let mut buffer = Buffer {
            buffer: VecDeque::with_capacity(1 + log.traces.len() + log.events.len()),
        };

        buffer.push(Ok(Some(Component::Meta(log.meta))));

        for trace in log.traces {
            buffer.push(Ok(Some(Component::Trace(trace))));
        }

        for event in log.events {
            buffer.push(Ok(Some(Component::Event(event))));
        }

        buffer
    }
}

impl Buffer {
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn push(&mut self, component: ResOpt) {
        self.buffer.push_back(component)
    }
}

#[cfg(test)]
mod tests {
    use crate::dev_util::load_example;
    use crate::stream;

    use super::*;

    #[test]
    fn test_buffer() {
        let mut buffer_a = load_example(&["book", "L1.xes"]);
        let mut buffer_b = Buffer::default();

        assert_eq!(buffer_a.len(), 7);
        assert_eq!(buffer_b.len(), 0);

        buffer_b.consume(&mut buffer_a).unwrap();

        assert_eq!(buffer_a.len(), 0);
        assert_eq!(buffer_b.len(), 7);

        let event = stream::Event::default();
        buffer_a.push(Ok(Some(stream::Component::Event(event))));

        assert_eq!(buffer_a.len(), 1);
        assert_eq!(buffer_b.len(), 7);

        buffer_b.consume(&mut buffer_a).unwrap();

        assert_eq!(buffer_a.len(), 0);
        assert_eq!(buffer_b.len(), 8);

        stream::void::consume(&mut buffer_b).unwrap();

        assert_eq!(buffer_a.len(), 0);
        assert_eq!(buffer_b.len(), 0);
    }

    #[test]
    fn test_buffer_error() {
        let mut buffer_a = load_example(&["non_parsing", "broken_xml.xes"]);
        let mut buffer_b = Buffer::default();

        assert!(buffer_b.consume(&mut buffer_a).is_err());
    }
}
