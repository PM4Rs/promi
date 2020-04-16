//! [_Process Mining_](https://en.wikipedia.org/wiki/Process_mining) to the
//! [_rust_](https://en.wikipedia.org/wiki/Rust_(programming_language)) programming language.
//!
//! # Application Scenarios
//!
//! ## XES validation
//! ```txt
//! XES file > XESReader > XesValidator > Sink
//! ```
//!
//! ## aggregate log data & model building
//! ```text
//! XES file > XesReader > XesValidator > Observer > Log | InductiveMiner
//!                                       - DFGGenerator | HeuristicMiner
//!                                       - FootprintGenerator | AlphaMiner
//! ```
//!
//! ## model assessment
//! ```text
//! Log | Buffer > Observer > Sink
//!                - TokenReplay
//! ```
//!
//! ## statistics
//! ```text
//! XES file > XesReader > StreamStats > XESWriter > stdout
//! ```
//!
//! ## network streaming
//! ```text
//! XES file > XesReader > BinaryWriter > network > BinaryReader > Log | InductiveMiner
//! ```
//!

#![warn(missing_docs)]

#[macro_use]
extern crate log as logging;
#[macro_use]
extern crate lazy_static;
extern crate chrono;
extern crate quick_xml;
extern crate regex;
extern crate thiserror;

pub mod error;
pub mod stream;

use std::convert::TryFrom;
use stream::{buffer, Element, Stream, StreamSink};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub type DateTime = chrono::DateTime<chrono::FixedOffset>;

#[derive(Debug, Clone)]
pub enum AttributeType {
    String(String),
    Date(DateTime),
    Int(i64),
    Float(f64),
    Boolean(bool),
    Id(String),
    List(Vec<Attribute>),
}

#[derive(Debug, Clone)]
pub enum Scope {
    Event,
    Trace
}

impl TryFrom<Option<String>> for Scope {
    type Error = crate::error::Error;

    fn try_from(value: Option<String>) -> Result<Self, Self::Error> {
        if let Some(s) = value {
            match s.as_str() {
                "trace" => Ok(Scope::Trace),
                "event" => Ok(Scope::Event),
                other=> Err(Self::Error::XesError(format!("Invalid scope: {:?}", other)))
            }
        } else {
            Ok(Scope::Event)
        }
    }
}

#[derive(Debug, Clone)]
pub struct Attribute {
    key: String,
    value: AttributeType,
}

#[derive(Debug, Clone)]
pub struct Extension {
    name: String,
    prefix: String,
    uri: String,
}

#[derive(Debug, Clone)]
pub struct Global {
    scope: Scope,
    attributes: Vec<Attribute>,
}

#[derive(Debug, Clone)]
pub struct Classifier {
    name: String,
    scope: Scope,
    keys: String,
}

#[derive(Debug, Clone)]
pub struct Event {
    attributes: Vec<Attribute>,
}

impl Default for Event {
    fn default() -> Self {
        Self {
            attributes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Trace {
    attributes: Vec<Attribute>,
    traces: Vec<Event>,
}

impl Default for Trace {
    fn default() -> Self {
        Self {
            attributes: Vec::new(),
            traces: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Log {
    extensions: Vec<Extension>,
    globals: Vec<Global>,
    classifiers: Vec<Classifier>,
    attributes: Vec<Attribute>,
    traces: Vec<Trace>,
    events: Vec<Event>,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            extensions: Vec::new(),
            globals: Vec::new(),
            classifiers: Vec::new(),
            attributes: Vec::new(),
            traces: Vec::new(),
            events: Vec::new(),
        }
    }
}

impl Into<buffer::Buffer> for Log {
    fn into(self) -> buffer::Buffer {
        let mut buffer = buffer::Buffer::default();

        for extension in self.extensions {
            buffer.push(Ok(Some(Element::Extension(extension))));
        }

        for global in self.globals {
            buffer.push(Ok(Some(Element::Global(global))));
        }

        for classifier in self.classifiers {
            buffer.push(Ok(Some(Element::Classifier(classifier))));
        }

        for attribute in self.attributes {
            buffer.push(Ok(Some(Element::Attribute(attribute))));
        }

        for trace in self.traces {
            buffer.push(Ok(Some(Element::Trace(trace))));
        }

        for event in self.events {
            buffer.push(Ok(Some(Element::Event(event))));
        }

        buffer
    }
}

impl StreamSink for Log {
    fn consume<T: Stream>(&mut self, source: &mut T) -> error::Result<()> {
        loop {
            match source.next_element()? {
                Some(Element::Extension(e)) => self.extensions.push(e),
                Some(Element::Global(g)) => self.globals.push(g),
                Some(Element::Classifier(c)) => self.classifiers.push(c),
                Some(Element::Attribute(a)) => self.attributes.push(a),
                Some(Element::Trace(t)) => self.traces.push(t),
                Some(Element::Event(e)) => self.events.push(e),
                None => break,
            }
        }

        Ok(())
    }
}

pub mod util {
    use std::path::{Path, PathBuf};

    pub fn expand_static(path: &[&str]) -> PathBuf {
        let mut exp = Path::new(env!("CARGO_MANIFEST_DIR")).join("static");

        for p in path.iter() {
            exp = exp.join(p);
        }

        exp
    }
}
