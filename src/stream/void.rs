//! Dummy stream and / or sink.
//!

use crate::stream::{AnyArtifact, ResOpt, Stream, StreamSink};
use crate::Result;

/// A dummy stream / sink that does nothing but producing an empty or consuming a given stream
pub struct Void;

impl Default for Void {
    fn default() -> Self {
        Void {}
    }
}

impl Stream for Void {
    fn inner_ref(&self) -> Option<&dyn Stream> {
        None
    }

    fn inner_mut(&mut self) -> Option<&mut dyn Stream> {
        None
    }

    fn next(&mut self) -> ResOpt {
        Ok(None)
    }
}

impl StreamSink for Void {}

/// Creates a dummy sink and consumes the given stream
pub fn consume<T: Stream>(stream: &mut T) -> Result<Vec<Vec<AnyArtifact>>> {
    Void::default().consume(stream)
}
