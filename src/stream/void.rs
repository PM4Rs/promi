//! Dummy stream and / or sink.
//!

use crate::stream::plugin::{Declaration, Entry, Factory, FactoryType, PluginProvider};
use crate::stream::{AnyArtifact, ResOpt, Sink, Stream};
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

impl Sink for Void {}

impl PluginProvider for Void {
    fn entries() -> Vec<Entry>
    where
        Self: Sized,
    {
        vec![
            Entry::new(
                "VoidStream",
                "A stream source that yields no items",
                Factory::new(
                    Declaration::default(),
                    FactoryType::Stream(Box::new(|_| Ok(Box::new(Void::default())))),
                ),
            ),
            Entry::new(
                "VoidSink",
                "A sink that discards all items",
                Factory::new(
                    Declaration::default(),
                    FactoryType::Sink(Box::new(|_| Ok(Box::new(Void::default())))),
                ),
            ),
        ]
    }
}

/// Creates a dummy sink and consumes the given stream
pub fn consume<T: Stream>(stream: &mut T) -> Result<Vec<Vec<AnyArtifact>>> {
    Void::default().consume(stream)
}
