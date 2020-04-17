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

/// promi's datetime type
pub type DateTime = chrono::DateTime<chrono::FixedOffset>;

/// promi version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Data types supported by attributes
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

/// Represents whether global or classifier target events or traces
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

/// Express atomic information
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > Information on any component (log, trace, or event) is stored in attribute components.
/// > Attributes describe the enclosing component, which may contain an arbitrary number of
/// > attributes.
///
#[derive(Debug, Clone)]
pub struct Attribute {
    key: String,
    value: AttributeType,
}

/// Provide semantics for sets of attributes
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > An extension defines a (possibly empty) set of attributes for every type of component.
/// > The extension provides points of reference for interpreting these attributes, and, thus, their
/// > components. Extensions, therefore, are primarily a vehicle for attaching semantics to a set of
/// > defined attributes per component.
///
#[derive(Debug, Clone)]
pub struct Extension {
    name: String,
    prefix: String,
    uri: String,
}

/// Global attributes and defaults
///
/// Globals define attributes that have to be present in target scope and provide default values for
/// such. This may either target traces or events, regardless whether within a trace or not.
///
#[derive(Debug, Clone)]
pub struct Global {
    scope: Scope,
    attributes: Vec<Attribute>,
}

/// Assigns an identity to trace or event
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > A classifier assigns an identity to each event that
/// > makes it comparable to others (via their assigned identity). Examples of such identities
/// > include the descriptive name of the event, the descriptive name of the case the event
/// > relates to, the descriptive name of the cause of the event, and the descriptive name of the
/// > case related to the event.
///
#[derive(Debug, Clone)]
pub struct Classifier {
    name: String,
    scope: Scope,
    keys: String,
}

/// Represents an atomic granule of activity that has been observed
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > An event component represents an atomic granule of activity that has been observed. If
/// > the event occurs in some trace, then it is clear to which case the event belongs. If the event
/// > does not occur in some trace, that is, if it occurs in the log, then we need ways to relate
/// > events to cases. For this, we will use the combination of a trace classifier and an event
/// > classifier.
///
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

/// Represents the execution of a single case
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > A trace component represents the execution of a single case, that is, of a single
/// > execution (or enactment) of the specific process. A trace shall contain a (possibly empty)
/// > list of events that are related to a single case. The order of the events in this list shall
/// > be important, as it signifies the order in which the events have been observed.
///
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

/// Represents information that is related to a specific process
///
/// From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
/// > A log component represents information that is related to a specific process. Examples
/// > of processes include handling insurance claims, using a complex X-ray machine, and browsing a
/// > website. A log shall contain a (possibly empty) collection of traces followed by a (possibly
/// > empty) list of events. The order of the events in this list shall be important, as it
/// > signifies the order in which the events have been observed. If the log contains only events
/// > and no traces, then the log is also called a stream.
///
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


/// Useful functions that may panic and are intended for developing promi.
///
pub mod util {
    use std::io;
    use std::fs;
    use std::path::{Path, PathBuf};

    /// Access assets
    ///
    /// For developing promi it's useful to work with some test files that are located in `/static`.
    /// In order to locate these in your system, this function exists. It takes a list of relative
    /// location descriptors and expands them to an absolute path.
    ///
    pub fn expand_static(path: &[&str]) -> PathBuf {
        let mut exp = Path::new(env!("CARGO_MANIFEST_DIR")).join("static");

        for p in path.iter() {
            exp = exp.join(p);
        }

        exp
    }

    /// Open a file as `io::BufReader`
    pub fn open_buffered(path: &Path) -> io::BufReader<fs::File> {
        io::BufReader::new(
            fs::File::open(&path).expect(format!("No such file {:?}", &path).as_str())
        )
    }
}
