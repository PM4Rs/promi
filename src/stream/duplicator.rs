//! Duplicate an event stream

use crate::stream::plugin::{Declaration, Entry, Factory, FactoryType, PluginProvider};
use crate::stream::{AnyArtifact, ResOpt, Sink, Stream};
use crate::Result;

/// Creates a copy of an extensible event stream on the fly
///
/// A duplicator forwards a stream while copying each component (inclusive errors) to forward them
/// to the given stream sink.
///
pub struct Duplicator<T: Stream, S: Sink> {
    stream: T,
    sink: S,
    open: bool,
}

impl<T: Stream, S: Sink> Duplicator<T, S> {
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

impl<T: Stream, S: Sink> Stream for Duplicator<T, S> {
    fn inner_ref(&self) -> Option<&dyn Stream> {
        Some(&self.stream)
    }

    fn inner_mut(&mut self) -> Option<&mut dyn Stream> {
        Some(&mut self.stream)
    }

    fn next(&mut self) -> ResOpt {
        if !self.open {
            self.open = true;
            self.sink.on_open()?;
        }

        match self.stream.next() {
            Ok(Some(component)) => {
                self.sink.on_component(component.clone())?;
                Ok(Some(component))
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

    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        self.sink.on_emit_artifacts()
    }
}

impl PluginProvider for Duplicator<Box<dyn Stream>, Box<dyn Sink>> {
    fn entries() -> Vec<Entry>
    where
        Self: Sized,
    {
        vec![Entry::new(
            "Duplicator",
            "Create an exact copy of an event stream",
            Factory::new(
                Declaration::default()
                    .stream("inner", "The stream to be copied")
                    .sink("copy", "The sink that consumes the copy"),
                FactoryType::Stream(Box::new(|parameters| -> Result<Box<dyn Stream>> {
                    Ok(Duplicator::new(
                        parameters.acquire_stream("inner")?,
                        parameters.acquire_sink("copy")?,
                    )
                    .into_boxed())
                })),
            ),
        )]
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::stream::tests::TestSink;
    use crate::stream::xes::XesReader;
    use crate::stream::Sink;

    use super::*;

    fn _test_sink_duplicator(path: PathBuf, counts: &[usize; 4], expect_error: bool) {
        let reader = XesReader::from(join_static_reader!(&path));
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
        let param = vec![
            (join_static!("xes", "book", "L1.xes"), [1, 7, 1, 0]),
            (join_static!("xes", "book", "L2.xes"), [1, 14, 1, 0]),
            (join_static!("xes", "book", "L3.xes"), [1, 5, 1, 0]),
            (join_static!("xes", "book", "L4.xes"), [1, 148, 1, 0]),
            (join_static!("xes", "book", "L5.xes"), [1, 15, 1, 0]),
            (
                join_static!("xes", "correct", "log_correct_attributes.xes"),
                [1, 1, 1, 0],
            ),
            (
                join_static!("xes", "correct", "event_correct_attributes.xes"),
                [1, 4, 1, 0],
            ),
        ];

        for (path, counts) in param {
            _test_sink_duplicator(path, &counts, false);
        }

        let param = vec![
            (
                join_static!("xes", "non_parsing", "boolean_incorrect_value.xes"),
                [1, 0, 0, 1],
            ),
            (
                join_static!("xes", "non_parsing", "broken_xml.xes"),
                [1, 6, 0, 1],
            ),
            (
                join_static!("xes", "non_parsing", "element_incorrect.xes"),
                [1, 0, 0, 1],
            ),
            (
                join_static!("xes", "non_parsing", "no_log.xes"),
                [1, 0, 0, 1],
            ),
            (
                join_static!("xes", "non_parsing", "global_incorrect_scope.xes"),
                [1, 0, 0, 1],
            ),
        ];

        for (path, counts) in param {
            _test_sink_duplicator(path, &counts, true);
        }
    }
}
