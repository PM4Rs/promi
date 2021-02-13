use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::stream::flow::segment::{PreparedSegment, Segment};
use crate::stream::flow::util::{ACNS, SCNS};
use crate::stream::{AnyArtifact, Artifact, Sink};
use crate::{Error, Result};

/// Pipe configuration
///
/// A pipe is a container for arbitrarily many (but at least) one stream segment and an optional
/// sink segment.
///
#[derive(Debug, Clone, serde::Serialize)]
pub struct Pipe {
    name: String,
    source_config: Segment,
    stream_configs: Vec<Segment>,
    sink_config: Option<Segment>,
}

impl Pipe {
    /// Create new named pipe with given source segment
    pub fn new<T: Into<String>>(name: T, source: Segment) -> Self {
        Self {
            name: name.into(),
            source_config: source,
            stream_configs: Vec::new(),
            sink_config: None,
        }
    }

    /// Add stream segment
    pub fn stream(&mut self, stream: Segment) -> &mut Self {
        self.stream_configs.push(stream);
        self
    }

    /// Add sink segment
    pub fn sink(&mut self, stream: Segment) -> &mut Self {
        self.sink_config = Some(stream);
        self
    }

    /// Apply all acquisitions, turning this into a prepared pipe
    pub(in crate::stream::flow) fn acquire(
        self,
        scns: &mut SCNS,
        acns: &mut ACNS,
    ) -> Result<PreparedPipe> {
        let sink = self.sink_config.unwrap_or_else(|| Segment::new("VoidSink"));
        Ok(PreparedPipe {
            name: self.name,
            source_builder: self.source_config.acquire(scns, acns)?,
            stream_builder: self
                .stream_configs
                .into_iter()
                .map(|c| c.acquire(scns, acns))
                .collect::<Result<_>>()?,
            sink_builder: sink.acquire(scns, acns)?,
        })
    }
}

impl Artifact for Pipe {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<I: IntoIterator<Item = Pipe> + erased_serde::Serialize + Debug + Any + Send> Artifact for I {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub(in crate::stream::flow) struct PreparedPipe {
    pub name: String,
    source_builder: PreparedSegment,
    stream_builder: Vec<PreparedSegment>,
    sink_builder: PreparedSegment,
}

impl PreparedPipe {
    pub fn execute(self) -> Result<Vec<(String, AnyArtifact)>> {
        // concatenate all segments
        let mut segments: Vec<_> = vec![self.source_builder]
            .into_iter()
            .chain(self.stream_builder)
            .chain(vec![self.sink_builder].into_iter())
            .collect();

        // acquire artifacts
        let mut artifacts = segments
            .iter_mut()
            .map(|cb| {
                Ok(cb
                    .receive_artifacts()?
                    .into_iter()
                    .unzip::<_, _, Vec<_>, Vec<_>>())
            })
            .collect::<Result<Vec<_>>>()?;

        // prepare senders for all artifact emissions
        let artifact_senders = segments
            .iter_mut()
            .map(|cb| cb.artifact_sender.drain(..).collect::<BTreeMap<_, _>>())
            .collect::<Vec<_>>();

        // assign artifact acquisitions to segments
        let mut segments = segments
            .into_iter()
            .zip(artifacts.iter_mut().map(|(_, a)| a))
            .peekable();

        // create stream/sink
        let mut stream = None;
        let mut sink = None;
        while let Some((segment, artifacts)) = segments.next() {
            if segments.peek().is_some() {
                stream = Some(segment.into_stream(artifacts.as_mut_slice(), stream)?);
            } else {
                sink = Some(segment.into_sink(artifacts.as_mut_slice())?);
            }
        }

        // consume stream
        let emissions = match (stream, sink) {
            (Some(mut stream), Some(mut sink)) => sink.consume(&mut stream)?,
            _ => unreachable!(),
        };

        // emit artifacts that where acquired somewhere else
        for (sender, artifacts) in artifact_senders.iter().zip(emissions.into_iter()) {
            for (s, a) in sender.values().zip(artifacts.into_iter()) {
                s.send(a)
                    .map_err(|e| Error::FlowError(format!("unable to send artifacts: {:?}", e)))?;
            }
        }

        // return remaining artifacts
        Ok(artifacts
            .into_iter()
            .map(|(k, a)| k.into_iter().zip(a.into_iter()))
            .flatten()
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[rustfmt::skip]
    fn test_execute() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let mut pipe = Pipe::new("Foo", Segment::new("VoidStream"));
        pipe.stream(Segment::new("Statistics")).sink(Segment::new("VoidSink"));

        let prepared_pipe = pipe.acquire(&mut scns, &mut acns).unwrap();
        let artifacts = prepared_pipe.execute().unwrap();

        assert!(artifacts.into_iter().next().is_none())
    }
}
