//! Buffering event streams.
//!

// standard library
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fs;
use std::io;
use std::path::Path;

// third party

// local
use crate::error::{Error, Result};
use crate::stream::xes::XesReader;
use crate::stream::{ResOpt, Stream, StreamSink};

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
    fn next(&mut self) -> ResOpt {
        match self.buffer.pop_front() {
            Some(element) => element,
            None => Ok(None),
        }
    }
}

impl StreamSink for Buffer {
    fn consume<T: Stream>(&mut self, source: &mut T) -> Result<()> {
        loop {
            match source.next()? {
                Some(element) => self.buffer.push_back(Ok(Some(element))),
                None => break,
            }
        }
        Ok(())
    }
}

impl Buffer {
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn push(&mut self, element: ResOpt) {
        self.buffer.push_back(element)
    }
}

pub fn load_example(path: &[&str]) -> Buffer {
    let mut root = Path::new(env!("CARGO_MANIFEST_DIR")).join("static");

    for p in path.iter() {
        root = root.join(p);
    }

    let f = io::BufReader::new(fs::File::open(&root).unwrap());
    let mut reader = XesReader::from(f);
    let mut buffer = Buffer::default();

    buffer.consume(&mut reader).unwrap();
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream;

    #[test]
    fn test_buffer() {
        let mut buffer_a = load_example(&["xes", "book", "L1.xes"]);
        let mut buffer_b = Buffer::default();

        assert_eq!(buffer_a.len(), 19);
        assert_eq!(buffer_b.len(), 0);

        buffer_b.consume(&mut buffer_a).unwrap();

        assert_eq!(buffer_a.len(), 0);
        assert_eq!(buffer_b.len(), 19);

        let event = crate::Event::default();
        buffer_a.push(Ok(Some(stream::Element::Event(event))));

        assert_eq!(buffer_a.len(), 1);
        assert_eq!(buffer_b.len(), 19);

        buffer_b.consume(&mut buffer_a).unwrap();

        assert_eq!(buffer_a.len(), 0);
        assert_eq!(buffer_b.len(), 20);

        stream::consume(&mut buffer_b).unwrap();

        assert_eq!(buffer_a.len(), 0);
        assert_eq!(buffer_b.len(), 0);
    }
}
