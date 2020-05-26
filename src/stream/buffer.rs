//! Buffering event streams.
//!

// standard library
use std::collections::VecDeque;
use std::fmt::Debug;

// third party

// local
use crate::error::{Error, Result};
use crate::stream::{Element, ResOpt, Stream, StreamSink};

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
    fn on_element(&mut self, element: Element) -> Result<()> {
        self.buffer.push_back(Ok(Some(element)));
        Ok(())
    }

    fn on_error(&mut self, error: Error) -> Result<()> {
        self.buffer.push_back(Err(error));
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::stream;
    use crate::stream::xes::XesReader;
    use std::fs;
    use std::io;
    use std::path::Path;

    pub fn load_example(path: &[&str]) -> Buffer {
        let mut root = Path::new(env!("CARGO_MANIFEST_DIR")).join("static");

        for p in path.iter() {
            root = root.join(p);
        }

        let f = io::BufReader::new(fs::File::open(&root).unwrap());
        let mut reader = XesReader::from(f);
        let mut buffer = Buffer::default();

        if buffer.consume(&mut reader).is_err() {
            eprintln!("an error occurred when loading example: {:?}", &root);
        }

        buffer
    }

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

    #[test]
    fn test_buffer_error() {
        let mut buffer_a = load_example(&["xes", "non_parsing", "broken_xml.xes"]);
        let mut buffer_b = Buffer::default();

        assert!(buffer_b.consume(&mut buffer_a).is_err());
    }
}
