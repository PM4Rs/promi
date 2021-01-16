use crate::stream::{AnyArtifact, ResOpt};
use crate::Result;

/// Extensible event stream
///
/// Yields one stream component at a time. Usually, it either acts as a factory or forwards another
/// stream. Errors are propagated to the caller.
///
pub trait Stream: Send {
    /// Get a reference to the inner stream if there is one
    fn inner_ref(&self) -> Option<&dyn Stream>;

    /// Get a mutable reference to the inner stream if there is one
    fn inner_mut(&mut self) -> Option<&mut dyn Stream>;

    /// Return the next stream component
    fn next(&mut self) -> ResOpt;

    /// Callback that releases artifacts of stream
    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        Ok(vec![])
    }

    /// Emit artifacts of stream
    ///
    /// A stream may aggregate data over time that is released by calling this method. Usually,
    /// this happens at the end of the stream.
    ///
    fn emit_artifacts(&mut self) -> Result<Vec<Vec<AnyArtifact>>> {
        let mut artifacts = Vec::new();
        if let Some(inner) = self.inner_mut() {
            artifacts.extend(Stream::emit_artifacts(inner)?);
        }
        artifacts.push(Stream::on_emit_artifacts(self)?);
        Ok(artifacts)
    }

    /// Turn stream instance into trait object
    fn into_boxed<'a>(self) -> Box<dyn Stream + 'a>
    where
        Self: Sized + 'a,
    {
        Box::new(self)
    }
}

impl<'a> Stream for Box<dyn Stream + 'a> {
    fn inner_ref(&self) -> Option<&dyn Stream> {
        self.as_ref().inner_ref()
    }

    fn inner_mut(&mut self) -> Option<&mut dyn Stream> {
        self.as_mut().inner_mut()
    }

    fn next(&mut self) -> ResOpt {
        self.as_mut().next()
    }

    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        self.as_mut().on_emit_artifacts()
    }
}
