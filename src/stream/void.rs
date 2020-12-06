//! Dummy stream and / or sink.
//!

use crate::stream::plugin::{Declaration, Factory, FactoryType, Plugin, RegistryEntry};
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

impl Plugin for Void {
    fn entries() -> Vec<RegistryEntry>
    where
        Self: Sized,
    {
        vec![
            RegistryEntry::new(
                "VoidStream",
                "A stream source that yields no items",
                Factory::new(
                    Declaration::default(),
                    FactoryType::Stream(Box::new(|_| Ok(Box::new(Void::default())))),
                ),
            ),
            RegistryEntry::new(
                "VoidSink",
                "A sink that discards all items",
                Factory::new(
                    Declaration::default(),
                    FactoryType::Stream(Box::new(|_| Ok(Box::new(Void::default())))),
                ),
            ),
        ]
    }
}

/// Creates a dummy sink and consumes the given stream
pub fn consume<T: Stream>(stream: &mut T) -> Result<Vec<Vec<AnyArtifact>>> {
    Void::default().consume(stream)
}
