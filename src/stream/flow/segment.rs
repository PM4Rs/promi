use std::collections::HashMap;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::stream::channel::{StreamReceiver, StreamSender};
use crate::stream::flow::util::{ArtifactReceiver, ArtifactSender, ACNS, SCNS};
use crate::stream::plugin::{AttrMap, REGISTRY};
use crate::stream::{AnyArtifact, Attribute, Sink, Stream};
use crate::{Error, Result};

/// Atomic unit of a pipe
///
/// A segment describes the configuration of a stream source, intermediate stream or a sink. It does
/// not make any assumptions about the soundness of the values passed though.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    name: String,
    attributes_: AttrMap,
    stream_sender: Vec<String>,
    stream_receiver: Vec<String>,
    artifact_sender: Vec<String>,
    artifact_receiver: Vec<String>,
}

impl Segment {
    /// Create new pipe
    pub fn new<N: Into<String>>(name: N) -> Self {
        Self {
            name: name.into(),
            attributes_: HashMap::new(),
            stream_sender: Vec::new(),
            stream_receiver: Vec::new(),
            artifact_sender: Vec::new(),
            artifact_receiver: Vec::new(),
        }
    }

    /// Add a single attribute to segment
    pub fn attribute(mut self, attribute: Attribute) -> Self {
        self.attributes_.insert(attribute.key, attribute.value);
        self
    }

    /// Add multiple attributes to segment
    pub fn attributes<A: IntoIterator<Item = Attribute>>(mut self, attributes: A) -> Self {
        attributes
            .into_iter()
            .map(|a| self.attributes_.insert(a.key, a.value))
            .for_each(drop);
        self
    }

    /// Acquire sending stream channel endpoint
    pub fn emit_stream<S: Into<String>>(mut self, sender: S) -> Self {
        self.stream_sender.push(sender.into());
        self
    }

    /// Acquire receiving stream channel endpoint
    pub fn acquire_stream<S: Into<String>>(mut self, receiver: S) -> Self {
        self.stream_receiver.push(receiver.into());
        self
    }

    /// Acquire sending artifact channel endpoint
    pub fn emit_artifact<S: Into<String>>(mut self, sender: S) -> Self {
        self.artifact_sender.push(sender.into());
        self
    }

    /// Acquire receiving artifact channel endpoint
    pub fn acquire_artifact<S: Into<String>>(mut self, receiver: S) -> Self {
        self.artifact_receiver.push(receiver.into());
        self
    }

    /// Acquire all channel endpoints, turning this into a prepared segment
    pub(in crate::stream::flow) fn acquire(
        self,
        scns: &mut SCNS,
        acns: &mut ACNS,
    ) -> Result<PreparedSegment> {
        Ok(PreparedSegment {
            name: self.name,
            attributes: self.attributes_,
            stream_sender: self
                .stream_sender
                .into_iter()
                .map(|k| {
                    let s = scns.acquire_sender(&k)?;
                    Ok((k, s))
                })
                .collect::<Result<_>>()?,
            stream_receiver: self
                .stream_receiver
                .into_iter()
                .map(|k| {
                    let r = scns.acquire_receiver(&k)?;
                    Ok((k, r))
                })
                .collect::<Result<_>>()?,
            artifact_sender: self
                .artifact_sender
                .into_iter()
                .map(|k| {
                    let s = acns.acquire_sender(&k)?;
                    Ok((k, s))
                })
                .collect::<Result<_>>()?,
            artifact_receiver: self
                .artifact_receiver
                .into_iter()
                .map(|k| {
                    let r = acns.acquire_receiver(&k)?;
                    Ok((k, r))
                })
                .collect::<Result<_>>()?,
        })
    }
}

pub(in crate::stream::flow) struct PreparedSegment {
    name: String,
    attributes: AttrMap,
    pub stream_sender: Vec<(String, StreamSender)>,
    pub stream_receiver: Vec<(String, StreamReceiver)>,
    pub artifact_sender: Vec<(String, ArtifactSender)>,
    pub artifact_receiver: Vec<(String, ArtifactReceiver)>,
}

impl PreparedSegment {
    pub fn receive_artifacts(&mut self) -> Result<Vec<(String, AnyArtifact)>> {
        self.artifact_receiver
            .drain(..)
            .map(|(k, r)| {
                let a = r.recv().map_err(|_| {
                    Error::FlowError(format!("unable to acquire artifact: {:?}", &k))
                })?;
                Ok((k, a))
            })
            .collect::<Result<_>>()
    }

    pub fn into_stream<'a>(
        self,
        artifacts: &'a mut [AnyArtifact],
        inner: Option<Box<dyn Stream + 'a>>,
    ) -> Result<Box<dyn Stream + 'a>> {
        let registry = REGISTRY.lock().map_err(|_| {
            Error::StreamError("unable to acquire stream plugin registry".to_string())
        })?;

        let entry = registry
            .get(&self.name)
            .ok_or_else(|| Error::FlowError(format!("no such stream plugin: {:?}", &self.name)))?;

        entry.factory.build_stream(
            self.attributes,
            artifacts,
            inner
                .into_iter()
                .chain(
                    self.stream_receiver
                        .into_iter()
                        .map(|(_, s)| -> Box<dyn Stream + 'a> { s.into_boxed() }),
                )
                .collect(),
            self.stream_sender
                .into_iter()
                .map(|(_, r)| -> Box<dyn Sink + 'a> { r.into_boxed() })
                .collect(),
        )
    }

    pub fn into_sink<'a>(self, artifacts: &'a mut [AnyArtifact]) -> Result<Box<dyn Sink + 'a>> {
        let registry = REGISTRY.lock().map_err(|_| {
            Error::StreamError("unable to acquire stream plugin registry".to_string())
        })?;

        let entry = registry
            .get(&self.name)
            .ok_or_else(|| Error::FlowError(format!("no such sink plugin: {:?}", &self.name)))?;

        entry.factory.build_sink(
            self.attributes,
            artifacts,
            self.stream_receiver
                .into_iter()
                .map(|(_, s)| -> Box<dyn Stream> { s.into_boxed() })
                .collect(),
            self.stream_sender
                .into_iter()
                .map(|(_, r)| -> Box<dyn Sink> { r.into_boxed() })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::stream::AttributeValue;

    use super::*;

    #[test]
    fn test_segment_acquire() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let segment = Segment::new("Foo")
            .acquire_artifact("Foo")
            .emit_artifact("Bar")
            .acquire_stream("Foo")
            .emit_stream("Bar");

        segment.acquire(&mut scns, &mut acns).unwrap();

        let a_snd: Vec<_> = acns
            .acquire_remaining_senders()
            .unwrap()
            .map(|(n, _)| n)
            .collect();
        let a_rcv: Vec<_> = acns
            .acquire_remaining_receivers()
            .unwrap()
            .map(|(n, _)| n)
            .collect();
        let s_snd: Vec<_> = scns
            .acquire_remaining_senders()
            .unwrap()
            .map(|(n, _)| n)
            .collect();
        let s_rcv: Vec<_> = scns
            .acquire_remaining_receivers()
            .unwrap()
            .map(|(n, _)| n)
            .collect();

        assert_eq!(a_snd, ["Foo"]);
        assert_eq!(a_rcv, ["Bar"]);
        assert_eq!(s_snd, ["Foo"]);
        assert_eq!(s_rcv, ["Bar"]);
    }

    #[test]
    #[should_panic(expected = r#"ChannelError("receiver \"Foo\" was already acquired")"#)]
    fn test_segment_acquire_artifact_error() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let segment = Segment::new("Foo")
            .acquire_artifact("Foo")
            .acquire_artifact("Foo");

        segment.acquire(&mut scns, &mut acns).unwrap();
    }

    #[test]
    #[should_panic(expected = r#"ChannelError("receiver \"Foo\" was already acquired")"#)]
    fn test_segment_acquire_stream_error() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let segment = Segment::new("Foo")
            .acquire_stream("Foo")
            .acquire_stream("Foo");

        segment.acquire(&mut scns, &mut acns).unwrap();
    }

    #[test]
    #[should_panic(expected = r#"ChannelError("sender \"Foo\" was already acquired")"#)]
    fn test_segment_emit_artifact_error() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let segment = Segment::new("Foo")
            .emit_artifact("Foo")
            .emit_artifact("Foo");

        segment.acquire(&mut scns, &mut acns).unwrap();
    }

    #[test]
    #[should_panic(expected = r#"ChannelError("sender \"Foo\" was already acquired")"#)]
    fn test_segment_emit_stream_error() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let segment = Segment::new("Foo").emit_stream("Foo").emit_stream("Foo");

        segment.acquire(&mut scns, &mut acns).unwrap();
    }

    #[test]
    fn test_prepared_segment_receive() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let segment = Segment::new("Foo")
            .acquire_artifact("Foo")
            .emit_artifact("Bar")
            .acquire_stream("Foo")
            .emit_stream("Bar");
        let mut prepared_segment = segment.acquire(&mut scns, &mut acns).unwrap();
        let sender = acns
            .acquire_remaining_senders()
            .unwrap()
            .map(|(_, s)| s)
            .next()
            .unwrap();

        sender.send(AttributeValue::Int(42).into()).unwrap();
        drop(sender);

        let mut artifacts = prepared_segment.receive_artifacts().unwrap().into_iter();
        let (r_key, r_atf) = artifacts.next().unwrap();

        assert_eq!(r_key, "Foo");
        assert_eq!(
            r_atf
                .downcast_ref::<AttributeValue>()
                .unwrap()
                .try_int()
                .unwrap(),
            &42i64
        );
        assert!(artifacts.next().is_none())
    }

    #[test]
    #[should_panic(expected = r#"FlowError("unable to acquire artifact: \"Foo\"")"#)]
    fn test_prepared_segment_receive_error() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let segment = Segment::new("Foo")
            .acquire_artifact("Foo")
            .emit_artifact("Bar")
            .acquire_stream("Foo")
            .emit_stream("Bar");
        let mut prepared_segment = segment.acquire(&mut scns, &mut acns).unwrap();

        acns.acquire_remaining_senders().unwrap().for_each(drop);
        prepared_segment.receive_artifacts().unwrap();
    }

    #[test]
    fn test_prepared_segment_into_stream() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let source_segment = Segment::new("VoidStream");
        let stream_segment = Segment::new("Statistics");
        let source_prepared = source_segment.acquire(&mut scns, &mut acns).unwrap();
        let stream_prepared = stream_segment.acquire(&mut scns, &mut acns).unwrap();

        let source = source_prepared.into_stream(&mut [], None).unwrap();
        stream_prepared.into_stream(&mut [], Some(source)).unwrap();
    }

    #[test]
    #[should_panic(expected = r#"FlowError("no such stream plugin: \"Foo\"")"#)]
    fn test_prepared_segment_into_stream_error() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let source_segment = Segment::new("Foo");
        let source_prepared = source_segment.acquire(&mut scns, &mut acns).unwrap();

        source_prepared.into_stream(&mut [], None).unwrap();
    }

    #[test]
    fn test_prepared_segment_into_sink() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let source_segment = Segment::new("VoidSink");
        let source_prepared = source_segment.acquire(&mut scns, &mut acns).unwrap();

        source_prepared.into_sink(&mut []).unwrap();
    }

    #[test]
    #[should_panic(expected = r#"FlowError("no such sink plugin: \"Foo\"")"#)]
    fn test_prepared_segment_into_sink_error() {
        let mut scns = SCNS::default();
        let mut acns = ACNS::default();

        scns.set_generation(0);
        acns.set_generation(0);

        let source_segment = Segment::new("Foo");
        let source_prepared = source_segment.acquire(&mut scns, &mut acns).unwrap();

        source_prepared.into_sink(&mut []).unwrap();
    }
}
