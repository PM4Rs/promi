//! Extensible Event Stream (XES) support for _promi_
//!
//! This module implements the XES standard version 1.0
//! ([IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html)). It supports a super
//! set of the XES for reading with optional validation. Resources on specification can be found at
//! [xes-standard.org](http://www.xes-standard.org/) and other than the shipped example files at
//! [processmining.org](http://www.processmining.org/logs/start).
//!
//! Things _promi_ handles differently to the standard:
//! * Additional attributes for elements are ignored rather than causing failure.
//! * String based attributes are not validated on import[1] automatically. This affects
//!     * `Attribute.key` (`xs:Name`)
//!     * `Extension.name` (`xs:NCName`)
//!     * `Extension.prefix` (`xs:NCName`)
//!     * `Extension.uri` (`xs:anyURI`)
//!     * `Global.scope` (`xs:NCName`)
//!     * `Classifier.name` (`xs:NCName`)
//!     * `Classifier.scope` (`xs:NCName`)
//!     * `Classifier.keys` (`xs:token`)
//!
//!     However, validation of these items is provided by `XesValidator`
//!
//! When running in trouble by parsing a XES file, consider validating against the official schema
//! definition first which is a simple bash one-liner (_xmllint_ required)
//!
//! ```bash
//! xmllint --schema http://www.xes-standard.org/xes-ieee-1849-2016.xsd file.xes --noout
//! ```
//!
//! [1] Files found in the wild sometimes tend to be not 100% standard compliant. This affects even
//!     examples from the popular processmining book.
//!
//! # Example
//! ```
//! use std::io;
//! use promi::stream::StreamSink;
//! use promi::stream::xes;
//!
//! let s = r#"<?xml version="1.0" encoding="UTF-8"?>
//!            <log xes.version="1.0" xes.features="">
//!                <trace>
//!                    <string key="id" value="Case1.0"/>
//!                    <event>
//!                        <string key="id" value="A"/>
//!                    </event>
//!                    <event>
//!                        <string key="id" value="B"/>
//!                    </event>
//!                </trace>
//!            </log>"#;
//!
//! let mut reader = xes::XesReader::from(io::BufReader::new(s.as_bytes()));
//! let mut writer = xes::XesWriter::new(io::stdout(), None, None);
//!
//! writer.consume(&mut reader).unwrap();
//! ```
//!

// standard library
use std::collections::HashMap;
use std::convert::{From, TryFrom};
use std::fmt::Debug;
use std::io;

// third party
use quick_xml::events::{
    BytesDecl as QxBytesDecl, BytesEnd as QxBytesEnd, BytesStart as QxBytesStart,
    BytesText as QxBytesText, Event as QxEvent,
};
use quick_xml::{Reader as QxReader, Writer as QxWriter};

// local
use crate::error::{Error, Result};
use crate::stream::xml_util::{validate_name, validate_ncname, validate_token, validate_uri};
use crate::stream::{Element, ResOpt, Stream, StreamSink};
use crate::{Scope, Attribute, AttributeType, Classifier, DateTime, Event, Extension, Global, Log, Trace};

fn parse_bool(string: &str) -> Result<bool> {
    match string {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
    .ok_or(Error::ParseBooleanError(string.to_string()))
}

#[derive(Debug)]
enum XesElement {
    Attribute(Attribute),
    Value(XesValue),
    Extension(Extension),
    Global(Global),
    Classifier(Classifier),
    Event(Event),
    Trace(Trace),
    Log(Log),
}

#[derive(Debug, Clone)]
struct XesValue {
    attributes: Vec<Attribute>,
}

impl TryFrom<XesIntermediate> for Attribute {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let key_str = intermediate.get_attr("key")?.clone();
        let val_str = intermediate.get_attr("value");

        let value = match intermediate.type_name.as_str() {
            "string" => Ok(AttributeType::String(val_str?.clone())),
            "date" => Ok(AttributeType::Date(DateTime::parse_from_rfc3339(
                val_str?.as_str(),
            )?)),
            "int" => Ok(AttributeType::Int(val_str?.parse::<i64>()?)),
            "float" => Ok(AttributeType::Float(val_str?.parse::<f64>()?)),
            "boolean" => Ok(AttributeType::Boolean(parse_bool(&val_str?.as_str())?)),
            "id" => Ok(AttributeType::Id(val_str?.clone())),
            "list" => {
                let mut attributes: Vec<Attribute> = Vec::new();

                for values in intermediate.elements {
                    match values {
                        XesElement::Value(v) => attributes.extend(v.attributes),
                        _ => ( /* ignore */ ),
                    }
                }

                Ok(AttributeType::List(attributes))
            }
            attr_key => Err(Error::KeyError(format!("unknown attribute {}", attr_key))),
        };

        Ok(Attribute {
            key: key_str,
            value: value?,
        })
    }
}

impl Attribute {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<usize> {
        let temp_string: String;

        let (tag, value) = match &self.value {
            AttributeType::String(value) => ("string", value.as_str()),
            AttributeType::Date(value) => {
                temp_string = value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);
                ("date", temp_string.as_str())
            }
            AttributeType::Int(value) => {
                temp_string = value.to_string();
                ("int", temp_string.as_str())
            }
            AttributeType::Float(value) => {
                temp_string = value.to_string();
                ("float", temp_string.as_str())
            }
            AttributeType::Boolean(value) => ("boolean", if *value { "true" } else { "false" }),
            AttributeType::Id(value) => ("id", value.as_str()),
            AttributeType::List(attributes) => {
                let mut bytes: usize = 0;
                let tag_l = b"list";
                let tag_v = b"values";
                let mut event_l = QxBytesStart::owned(tag_l.to_vec(), tag_l.len());
                let mut event_v = QxBytesStart::owned(tag_v.to_vec(), tag_v.len());

                event_l.push_attribute(("key", validate_name(self.key.as_str())?));

                bytes += writer.write_event(QxEvent::Start(event_l))?;
                bytes += writer.write_event(QxEvent::Start(event_v))?;

                for attribute in attributes.iter() {
                    bytes += attribute.write_xes(writer)?;
                }

                bytes += writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag_v)))?;
                bytes += writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag_l)))?;

                return Ok(bytes);
            }
        };

        let tag = tag.as_bytes();
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        event.push_attribute(("key", validate_name(self.key.as_str())?));
        event.push_attribute(("value", value));

        Ok(writer.write_event(QxEvent::Empty(event))?)
    }
}

impl TryFrom<XesIntermediate> for XesValue {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let mut attributes: Vec<Attribute> = Vec::new();

        for element in intermediate.elements {
            match element {
                XesElement::Attribute(attribute) => attributes.push(attribute),
                other => warn!("unexpected child element of value: {:?}, ignore", other),
            }
        }

        Ok(XesValue { attributes })
    }
}

impl TryFrom<XesIntermediate> for Extension {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        Ok(Extension {
            name: intermediate.get_attr("name")?.clone(),
            prefix: intermediate.get_attr("prefix")?.clone(),
            uri: intermediate.get_attr("uri")?.clone(),
        })
    }
}

impl Extension {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<usize> {
        let tag = b"extension";
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        event.push_attribute(("name", validate_ncname(self.name.as_str())?));
        event.push_attribute(("prefix", validate_ncname(self.prefix.as_str())?));
        event.push_attribute(("uri", validate_uri(self.uri.as_str())?));

        Ok(writer.write_event(QxEvent::Empty(event))?)
    }
}

impl TryFrom<XesIntermediate> for Global {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let scope = Scope::try_from(intermediate.attributes.get("scope").cloned())?;
        let mut attributes: Vec<Attribute> = Vec::new();

        for element in intermediate.elements {
            match element {
                XesElement::Attribute(attribute) => attributes.push(attribute),
                other => warn!("unexpected child element of global: {:?}, ignore", other),
            }
        }

        Ok(Global { scope, attributes })
    }
}

impl Global {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<usize> {
        let tag = b"global";
        let mut bytes: usize = 0;
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        match self.scope {
            Scope::Event => event.push_attribute(("scope", "event")),
            Scope::Trace => event.push_attribute(("scope", "trace"))
        }

        bytes += writer.write_event(QxEvent::Start(event))?;

        for attribute in self.attributes.iter() {
            bytes += attribute.write_xes(writer)?;
        }

        bytes += writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag)))?;

        Ok(bytes)
    }
}

impl TryFrom<XesIntermediate> for Classifier {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        Ok(Classifier {
            name: intermediate.get_attr("name")?.clone(),
            scope: Scope::try_from(intermediate.attributes.get("scope").cloned())?,
            keys: intermediate.get_attr("keys")?.clone(),
        })
    }
}

impl Classifier {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<usize> {
        let tag = b"classifier";
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        event.push_attribute(("name", validate_ncname(self.name.as_str())?));
        match self.scope {
            Scope::Event => event.push_attribute(("scope", "event")),
            Scope::Trace => event.push_attribute(("scope", "trace"))
        }
        event.push_attribute(("keys", validate_token(self.keys.as_str())?));

        Ok(writer.write_event(QxEvent::Empty(event))?)
    }
}

impl TryFrom<XesIntermediate> for Event {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let mut attributes: Vec<Attribute> = Vec::new();

        for element in intermediate.elements {
            match element {
                XesElement::Attribute(attribute) => attributes.push(attribute),
                other => warn!("unexpected child element of event: {:?}, ignore", other),
            }
        }

        Ok(Event { attributes })
    }
}

impl Event {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<usize> {
        let tag = b"event";
        let mut bytes: usize = 0;
        let event = QxBytesStart::owned(tag.to_vec(), tag.len());

        bytes += writer.write_event(QxEvent::Start(event))?;

        for attribute in self.attributes.iter() {
            bytes += attribute.write_xes(writer)?;
        }

        bytes += writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag)))?;

        Ok(bytes)
    }
}

impl TryFrom<XesIntermediate> for Trace {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let mut attributes: Vec<Attribute> = Vec::new();
        let mut traces: Vec<Event> = Vec::new();

        for element in intermediate.elements {
            match element {
                XesElement::Attribute(attribute) => attributes.push(attribute),
                XesElement::Event(event) => traces.push(event),
                other => warn!("unexpected child element of trace: {:?}, ignore", other),
            }
        }

        Ok(Trace { attributes, traces })
    }
}

impl Trace {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<usize> {
        let tag = b"trace";
        let mut bytes: usize = 0;
        let event = QxBytesStart::owned(tag.to_vec(), tag.len());

        bytes += writer.write_event(QxEvent::Start(event))?;

        for attribute in self.attributes.iter() {
            bytes += attribute.write_xes(writer)?;
        }

        for trace in self.traces.iter() {
            bytes += trace.write_xes(writer)?;
        }

        bytes += writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag)))?;

        Ok(bytes)
    }
}

impl TryFrom<XesIntermediate> for Log {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let mut extensions: Vec<Extension> = Vec::new();
        let mut globals: Vec<Global> = Vec::new();
        let mut classifiers: Vec<Classifier> = Vec::new();
        let mut attributes: Vec<Attribute> = Vec::new();
        let mut traces: Vec<Trace> = Vec::new();
        let mut events: Vec<Event> = Vec::new();

        for element in intermediate.elements {
            match element {
                XesElement::Extension(extension) => extensions.push(extension),
                XesElement::Global(global) => globals.push(global),
                XesElement::Classifier(classifier) => classifiers.push(classifier),
                XesElement::Attribute(attribute) => attributes.push(attribute),
                XesElement::Trace(trace) => traces.push(trace),
                XesElement::Event(event) => events.push(event),
                other => warn!("unexpected child element of log: {:?}, ignore", other),
            }
        }

        Ok(Log {
            extensions,
            globals,
            classifiers,
            attributes,
            traces,
            events,
        })
    }
}

impl TryFrom<XesIntermediate> for XesElement {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        match intermediate.type_name.as_str() {
            "string" | "date" | "int" | "float" | "boolean" | "id" | "list" => {
                Ok(XesElement::Attribute(Attribute::try_from(intermediate)?))
            }
            "values" => Ok(XesElement::Value(XesValue::try_from(intermediate)?)),
            "extension" => Ok(XesElement::Extension(Extension::try_from(intermediate)?)),
            "global" => Ok(XesElement::Global(Global::try_from(intermediate)?)),
            "classifier" => Ok(XesElement::Classifier(Classifier::try_from(intermediate)?)),
            "event" => Ok(XesElement::Event(Event::try_from(intermediate)?)),
            "trace" => Ok(XesElement::Trace(Trace::try_from(intermediate)?)),
            "log" => Ok(XesElement::Log(Log::try_from(intermediate)?)),
            other => Err(Error::XesError(format!("unexpected XES element: {:?}", other))),
        }
    }
}

/// A container for XES element data used during parsing to collect attributes and child elements
#[derive(Debug)]
struct XesIntermediate {
    type_name: String,
    attributes: HashMap<String, String>,
    elements: Vec<XesElement>,
}

impl XesIntermediate {
    fn from_event(event: QxBytesStart) -> Result<Self> {
        let mut attr: HashMap<String, String> = HashMap::new();

        for attribute in event.attributes() {
            let attribute = attribute?;
            attr.insert(
                String::from_utf8(attribute.key.to_vec())?,
                String::from_utf8(attribute.value.to_vec())?,
            );
        }

        Ok(XesIntermediate {
            type_name: String::from_utf8(event.name().to_vec())?,
            attributes: attr,
            elements: Vec::new(),
        })
    }

    fn get_attr(&self, key: &str) -> Result<&String> {
        match self.attributes.get(key) {
            Some(value) => Ok(value),
            None => {
                let msg = format!("missing {:?} attribute in {:?}", key, self.type_name);
                Err(Error::KeyError(msg))
            }
        }
    }

    fn add_element(&mut self, element: XesElement) {
        self.elements.push(element)
    }
}

pub struct XesReader<R: io::BufRead> {
    reader: QxReader<R>,
    buffer: Vec<u8>,
    stack: Vec<XesIntermediate>,
}

impl<R: io::BufRead> XesReader<R> {
    pub fn new(reader: R) -> Self {
        XesReader {
            reader: QxReader::from_reader(reader),
            buffer: Vec::new(),
            stack: Vec::new(),
        }
    }
}

impl<R: io::BufRead> From<R> for XesReader<R> {
    fn from(reader: R) -> Self {
        XesReader::new(reader)
    }
}

impl<T: io::BufRead> Stream for XesReader<T> {
    fn next_element(&mut self) -> ResOpt {
        let mut top_level_element: Option<XesElement> = None;

        loop {
            match self.reader.read_event(&mut self.buffer) {
                Ok(QxEvent::Start(event)) => self.stack.push(XesIntermediate::from_event(event)?),
                Ok(QxEvent::End(_event)) => {
                    let intermediate = self.stack.pop().unwrap();
                    let element = XesElement::try_from(intermediate)?;

                    if self.stack.len() == 1 {
                        top_level_element = Some(element);
                        break;
                    } else {
                        match self.stack.last_mut() {
                            Some(intermediate) => intermediate.add_element(element),
                            None => break,
                        }
                    }
                }
                Ok(QxEvent::Empty(event)) => {
                    let element = {
                        let intermediate = XesIntermediate::from_event(event)?;
                        XesElement::try_from(intermediate)?
                    };

                    if self.stack.len() == 1 {
                        top_level_element = Some(element);
                        break;
                    } else {
                        match self.stack.last_mut() {
                            Some(intermediate) => intermediate.add_element(element),
                            None => break,
                        }
                    }
                }
                Err(e) => {
                    return Err(Error::XesError(format!(
                        "Error at position {}: {:?}",
                        self.reader.buffer_position(),
                        e
                    )));
                }
                Ok(QxEvent::Eof) => {
                    if self.buffer.len() == 0 {
                        return Err(Error::XesError(String::from("No root element found")));
                    }
                    break;
                }
                _ => (),
            }

            self.buffer.clear();
        }

        match top_level_element {
            Some(XesElement::Extension(e)) => Ok(Some(Element::Extension(e))),
            Some(XesElement::Global(g)) => Ok(Some(Element::Global(g))),
            Some(XesElement::Classifier(c)) => Ok(Some(Element::Classifier(c))),
            Some(XesElement::Attribute(a)) => Ok(Some(Element::Attribute(a))),
            Some(XesElement::Trace(t)) => Ok(Some(Element::Trace(t))),
            Some(XesElement::Event(e)) => Ok(Some(Element::Event(e))),
            Some(e) => Err(Error::XesError(format!(
                "XES element is not supported for streaming: {:?}",
                e
            ))),
            None => Ok(None),
        }
    }
}

pub struct XesWriter<W: io::Write> {
    writer: QxWriter<W>,
    bytes_written: usize,
}

impl<W: io::Write> XesWriter<W> {
    pub fn new(writer: W, indent_char: Option<u8>, indent_size: Option<usize>) -> Self {
        let writer = QxWriter::new_with_indent(
            writer,
            indent_char.unwrap_or(b'\t'),
            indent_size.unwrap_or(1),
        );

        XesWriter {
            writer,
            bytes_written: 0,
        }
    }
}

impl<W: io::Write> StreamSink for XesWriter<W> {
    fn consume<T: Stream>(&mut self, source: &mut T) -> Result<()> {
        // XML declaratioin
        let declaration = QxBytesDecl::new(b"1.0", Some(b"UTF-8"), None);
        self.bytes_written += self.writer.write_event(QxEvent::Decl(declaration))?;

        // write comments
        self.bytes_written += [
            format!(
                " This file has been generated with promi {} ",
                crate::VERSION
            )
            .as_str(),
            " It conforms to the XML serialization of the XES standard (IEEE Std 1849-2016) ",
            " For log storage and management, see http://www.xes-standard.org. ",
            " promi is available at https://crates.io/crates/promi ",
        ]
        .iter()
        .map(|s| {
            self.writer
                .write_event(QxEvent::Comment(QxBytesText::from_plain_str(s)))
        })
        .fold(Ok(0), |s: Result<usize>, v| Ok(s? + v?))?;

        // write contents
        let tag = b"log";
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        event.push_attribute(("xes.version", "1849.2016"));
        event.push_attribute(("xes.features", ""));

        self.bytes_written += self.writer.write_event(QxEvent::Start(event))?;

        loop {
            self.bytes_written += match source.next_element()? {
                Some(Element::Extension(e)) => e.write_xes(&mut self.writer)?,
                Some(Element::Global(g)) => g.write_xes(&mut self.writer)?,
                Some(Element::Classifier(c)) => c.write_xes(&mut self.writer)?,
                Some(Element::Attribute(a)) => a.write_xes(&mut self.writer)?,
                Some(Element::Trace(t)) => t.write_xes(&mut self.writer)?,
                Some(Element::Event(e)) => e.write_xes(&mut self.writer)?,
                None => break,
            }
        }

        self.bytes_written += self
            .writer
            .write_event(QxEvent::End(QxBytesEnd::borrowed(tag)))?;
        self.bytes_written += self.writer.write_event(QxEvent::Eof)?;

        Ok(())
    }
}

impl<W: io::Write> XesWriter<W> {
    fn inner(&mut self) -> &W {
        self.writer.inner()
    }

    fn into_inner(self) -> W {
        self.writer.into_inner()
    }
}

pub struct XesValidator { /* TODO to be implemented */ }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::consume;
    use crate::stream::buffer::Buffer;
    use crate::util::expand_static;
    use std::io;
    use std::io::Write;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio, Output};

    #[test]
    fn test_parse_bool() {
        assert!(parse_bool("true").unwrap());
        assert!(parse_bool("1").unwrap());
        assert!(!parse_bool("false").unwrap());
        assert!(!parse_bool("0").unwrap());
        assert!(parse_bool("").is_err());
        assert!(parse_bool("jein").is_err());
    }

    fn deserialize_dir(path: PathBuf, expect_failure: bool) {
        for p in fs::read_dir(path).unwrap().map(|p| p.unwrap()) {
            let f = io::BufReader::new(fs::File::open(&p.path()).unwrap());
            let mut reader = XesReader::from(f);
            let result = consume(&mut reader);

            if expect_failure {
                assert!(
                    result.is_err(),
                    format!("parsing {:?} is expected to fail but didn't", p.path())
                );
            } else {
                assert!(
                    result.is_ok(),
                    format!("parsing {:?} unexpectedly failed: {:?}", p.path(), result.err())
                );
            }
        }
    }

    // Parse files that comply with the standard.
    #[test]
    fn test_deserialize_correct() {
        deserialize_dir(expand_static(&["xes", "correct"]), false);
    }

    // Parse files that technically don't comply with the standard but can be parsed safely.
    #[test]
    fn test_deserialize_recoverable() {
        deserialize_dir(expand_static(&["xes", "recoverable"]), false);
    }

    // Import incorrect files, expecting Failure.
    #[test]
    fn test_deserialize_non_parsing() {
        deserialize_dir(expand_static(&["xes", "non_parsing"]), true);
    }

    // Import incorrect files that parse successfully. Most of these error classes can be caught by
    // XesValidator.
    #[test]
    fn test_deserialize_non_validating() {
        deserialize_dir(expand_static(&["xes", "non_validating"]), false);
    }

    fn validate_xes(xes: &[u8]) -> Output {
        let mut child = Command::new("xmllint")
            .arg("--noout")
            .arg("--schema")
            .arg(expand_static(&["xes", "xes-ieee-1849-2016.xsd"]))
            .arg("-")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn().unwrap();

        child.stdin.as_mut().unwrap().write_all(xes).unwrap();
        child.wait_with_output().unwrap()
    }

    fn validate_dir(path: PathBuf) {
        for p in fs::read_dir(path).unwrap().map(|p| p.unwrap()) {
            let f = io::BufReader::new(fs::File::open(&p.path()).unwrap());
            let mut buffer = Buffer::default();
            let mut snapshots: Vec<Vec<u8>> = Vec::new();

            buffer.consume(&mut XesReader::from(f)).unwrap();

            // serialize to XML
            let bytes: Vec<u8> = Vec::new();
            let mut writer = XesWriter::new(bytes, None, None);
            writer.consume(&mut buffer).unwrap();

            let validation_result = validate_xes(&writer.into_inner()[..]);

            assert!(
                validation_result.status.success(),
                format!("validation failed for {:?}, {:?}", p, validation_result)
            );

            break;
        }
    }

    // Test whether serialization to XES XML representation yield syntactically correct results.
    // This test requires `xmllint` to be available in path.
    #[test]
    fn test_serialize_syntax() {
        validate_dir(expand_static(&["xes", "correct"]));
        validate_dir(expand_static(&["xes", "recoverable"]));
    }

    fn serde_loop_dir(path: PathBuf) {
        for p in fs::read_dir(path).unwrap().map(|p| p.unwrap()) {
            let f = io::BufReader::new(fs::File::open(&p.path()).unwrap());
            let mut buffer = Buffer::default();
            let mut snapshots: Vec<Vec<u8>> = Vec::new();

            buffer.consume(&mut XesReader::from(f)).unwrap();

            for i in 0..2 {
                // serialize to XML
                let bytes: Vec<u8> = Vec::new();
                let mut writer = XesWriter::new(bytes, None, None);
                writer.consume(&mut buffer).unwrap();

                // make snapshot
                let bytes = writer.into_inner();
                snapshots.push(bytes.clone());

                // deserialize from XML
                let mut reader = XesReader::from(io::Cursor::new(bytes));
                buffer.consume(&mut reader).unwrap();
            }

            for (s1, s2) in snapshots.iter().zip(&snapshots[1..]) {
                assert_eq!(s1, s2)
            }
        }
    }

    // Test whether serialization to and deserialization from XES XML representation preserves
    // semantics.
    #[test]
    fn test_serialize_semantics() {
        serde_loop_dir(expand_static(&["xes", "correct"]));
        serde_loop_dir(expand_static(&["xes", "recoverable"]));
    }

    // Validate correct XES. Must not fail.
    #[test]
    fn test_validate_correct() {
        unimplemented!()
    }

    // Validate incorrect XES. Must fail.
    #[test]
    fn test_validate_non_validating() {
        unimplemented!()
    }
}
