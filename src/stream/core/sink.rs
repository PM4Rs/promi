use crate::stream::{AnyArtifact, Component, Stream};
use crate::{Error, Result};

/// Stream endpoint
///
/// A stream sink acts as an endpoint for an extensible event stream and is usually used when a
/// stream is converted into different representation. If that's not intended, the `void::consume`
/// function is a good shortcut. It simply discards the stream's contents.
///
pub trait Sink: Send {
    /// Optional callback that is invoked when the stream is opened
    fn on_open(&mut self) -> Result<()> {
        Ok(())
    }

    /// Callback that is invoked on each stream component
    fn on_component(&mut self, _component: Component) -> Result<()> {
        Ok(())
    }

    /// Optional callback that is invoked once the stream is closed
    fn on_close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Optional callback that is invoked when an error occurs
    fn on_error(&mut self, _error: Error) -> Result<()> {
        Ok(())
    }

    /// Emit artifacts of stream sink
    ///
    /// A stream sink may aggregate data over time that is released by calling this method. Usually,
    /// this happens at the end of the stream.
    ///
    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        Ok(vec![])
    }

    /// Invokes a stream as long as it provides new components.
    fn consume(&mut self, stream: &mut dyn Stream) -> Result<Vec<Vec<AnyArtifact>>> {
        // call pre-execution hook
        self.on_open()?;

        // consume stream
        loop {
            match stream.next() {
                Ok(Some(component)) => self.on_component(component)?,
                Ok(None) => break,
                Err(error) => {
                    self.on_error(error.clone())?;
                    return Err(error);
                }
            };
        }

        // call post-execution hook
        self.on_close()?;

        // collect artifacts
        let mut artifacts = Stream::emit_artifacts(stream)?;
        artifacts.push(Sink::on_emit_artifacts(self)?);
        Ok(artifacts)
    }

    /// Turn sink instance into trait object
    fn into_boxed<'a>(self) -> Box<dyn Sink + 'a>
    where
        Self: Sized + 'a,
    {
        Box::new(self)
    }
}

impl<'a> Sink for Box<dyn Sink + 'a> {
    fn on_open(&mut self) -> Result<()> {
        self.as_mut().on_open()
    }

    fn on_component(&mut self, component: Component) -> Result<()> {
        self.as_mut().on_component(component)
    }

    fn on_close(&mut self) -> Result<()> {
        self.as_mut().on_close()
    }

    fn on_error(&mut self, error: Error) -> Result<()> {
        self.as_mut().on_error(error)
    }

    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        self.as_mut().on_emit_artifacts()
    }
}
