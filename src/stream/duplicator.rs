//! Duplicate an event stream

// standard library

// third party
use rand::{distributions::Open01, random, Rng};
use rand_pcg::Pcg64;

// local
use crate::stream::{Element, ResOpt, Stream, StreamSink, WrappingStream};

/// Creates a copy of an extensible event stream on the fly
///
/// A duplicator forwards a stream while copying each element (and errors) to forward them to the
/// given stream sink.
///
pub struct Duplicator<T: Stream, S: StreamSink> {
    stream: T,
    sink: S,
    open: bool,
}

impl<T: Stream, S: StreamSink> Duplicator<T, S> {
    /// Create a new duplicator
    pub fn new(stream: T, sink: S) -> Self {
        Duplicator {
            stream,
            sink,
            open: false,
        }
    }

    /// Drop duplicator and release sink
    pub fn into_sink(self) -> S {
        self.sink
    }
}

impl<T: Stream, S: StreamSink> Stream for Duplicator<T, S> {
    fn next(&mut self) -> ResOpt {
        if !self.open {
            self.open = true;
            self.sink.on_open()?;
        }

        match self.stream.next() {
            Ok(Some(element)) => {
                self.sink.on_element(element.clone())?;
                Ok(Some(element))
            }
            Ok(None) => {
                self.sink.on_close()?;
                Ok(None)
            }
            Err(error) => {
                self.sink.on_error(error.clone())?;
                Err(error)
            }
        }
    }
}

impl<T: Stream, S: StreamSink> WrappingStream<T> for Duplicator<T, S> {
    fn inner(&self) -> &T {
        &self.stream
    }

    fn into_inner(self) -> T {
        self.stream
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_util::{expand_static, open_buffered};
    use crate::stream::tests::TestSink;
    use crate::stream::xes::XesReader;
    use crate::stream::StreamSink;
    use std::path::PathBuf;

    fn _test_sink_duplicator(path: PathBuf, counts: &[usize; 4], expect_error: bool) {
        let f = open_buffered(&path);
        let reader = XesReader::from(f);
        let sink_1 = TestSink::default();
        let mut sink_2 = TestSink::default();
        let mut duplicator = Duplicator::new(reader, sink_1);

        assert_eq!(sink_2.consume(&mut duplicator).is_err(), expect_error);

        let sink_1 = duplicator.into_sink();

        assert_eq!(&sink_1.counts(), counts);
        assert_eq!(&sink_2.counts(), counts);
    }

    #[test]
    fn test_sink_duplicator() {
        let param = [
            // open element close error
            ("book", "L1.xes", [1, 7, 1, 0]),
            ("book", "L2.xes", [1, 14, 1, 0]),
            ("book", "L3.xes", [1, 5, 1, 0]),
            ("book", "L4.xes", [1, 148, 1, 0]),
            ("book", "L5.xes", [1, 15, 1, 0]),
            ("correct", "log_correct_attributes.xes", [1, 1, 1, 0]),
            ("correct", "event_correct_attributes.xes", [1, 4, 1, 0]),
        ];

        for (d, f, counts) in param.iter() {
            _test_sink_duplicator(expand_static(&["xes", d, f]), counts, false);
        }

        let param = [
            ("non_parsing", "boolean_incorrect_value.xes", [1, 0, 0, 1]),
            ("non_parsing", "broken_xml.xes", [1, 6, 0, 1]),
            ("non_parsing", "element_incorrect.xes", [1, 0, 0, 1]),
            ("non_parsing", "no_log.xes", [1, 0, 0, 1]),
            ("non_parsing", "global_incorrect_scope.xes", [1, 0, 0, 1]),
        ];

        for (d, f, counts) in param.iter() {
            _test_sink_duplicator(expand_static(&["xes", d, f]), counts, true);
        }
    }
}