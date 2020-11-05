//! XML serialization and deserialization of XES
//!
//! This module implements XML serialization, deserialization and validation of XES
//! (IEEE Std 1849-2016). In fact, for reading it understands a super set of XES due to technical
//! simplicity and compatibility with older or broken instances. However, for writing we aim to be
//! 100% compliant (assuming validation is turned on). Hence, if you run into a case where invalid
//! XES XML is produced consider it a bug and feel invited to report an issue.
//!
//! For further information see [xes-standard.org](http://www.xes-standard.org/) and for other than
//! the shipped example files see [processmining.org](http://www.processmining.org/logs/start).
//!
//! When having trouble while parsing a XES file, consider validating against the official schema
//! definition first which is a simple bash one-liner (_xmllint_ required):
//!
//! ```bash
//! xmllint \
//!     --noout \
//!     --schema http://www.xes-standard.org/downloads/xes-ieee-1849-2016-April-15-2020.xsd \
//!     file.xes
//! ```
//!
//! # Example
//! This example illustrates how to serialize XES XML from a string and deserialize it to stdout.
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

use std::collections::{BTreeMap, HashMap};
use std::convert::{From, TryFrom};
use std::fmt::Debug;
use std::io;

use quick_xml::{Reader as QxReader, Result as QxResult, Writer as QxWriter};
use quick_xml::events::{
    BytesDecl as QxBytesDecl, BytesEnd as QxBytesEnd, BytesStart as QxBytesStart,
    BytesText as QxBytesText, Event as QxEvent,
};

use crate::{DateTime, Error, Result};
use crate::stream::{
    Attribute, AttributeValue, ClassifierDecl, Component, Event, ExtensionDecl, Global, Meta,
    ResOpt, Scope, Stream, StreamSink, Trace,
};
use crate::stream::log::Log;
use crate::stream::xml_util::{
    parse_bool, validate_name, validate_ncname, validate_token, validate_uri,
};

#[derive(Debug)]
enum XesComponent {
    Attribute(Attribute),
    Value(XesValue),
    ExtensionDecl(ExtensionDecl),
    Global(Global),
    ClassifierDecl(ClassifierDecl),
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

    fn try_from(mut intermediate: XesIntermediate) -> Result<Self> {
        let key_str = intermediate.pop("key")?;
        let val_str = intermediate.pop("value");

        let value = match intermediate.type_name.as_str() {
            "string" => AttributeValue::String(val_str?),
            "date" => AttributeValue::Date(DateTime::parse_from_rfc3339(val_str?.as_str())?),
            "int" => AttributeValue::Int(val_str?.parse::<i64>()?),
            "float" => AttributeValue::Float(val_str?.parse::<f64>()?),
            "boolean" => AttributeValue::Boolean(parse_bool(&val_str?.as_str())?),
            "id" => AttributeValue::Id(val_str?),
            "list" => {
                let mut attributes: Vec<Attribute> = Vec::new();

                for component in intermediate.components {
                    if let XesComponent::Value(value) = component {
                        attributes.extend(value.attributes);
                    }
                }

                AttributeValue::List(attributes)
            }
            attr_key => return Err(Error::KeyError(format!("unknown attribute {}", attr_key))),
        };

        Ok(Attribute::new(key_str, value))
    }
}

impl Attribute {
    fn write_xes_kv<W: io::Write>(
        key: &str,
        value: &AttributeValue,
        writer: &mut QxWriter<W>,
    ) -> Result<()> {
        let temp_string: String;

        let (tag, value) = match value {
            AttributeValue::String(value) => ("string", value.as_str()),
            AttributeValue::Date(value) => {
                temp_string = value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);
                ("date", temp_string.as_str())
            }
            AttributeValue::Int(value) => {
                temp_string = value.to_string();
                ("int", temp_string.as_str())
            }
            AttributeValue::Float(value) => {
                temp_string = value.to_string();
                ("float", temp_string.as_str())
            }
            AttributeValue::Boolean(value) => ("boolean", if *value { "true" } else { "false" }),
            AttributeValue::Id(value) => ("id", value.as_str()),
            AttributeValue::List(attributes) => {
                let tag_l = b"list";
                let tag_v = b"values";
                let mut event_l = QxBytesStart::owned(tag_l.to_vec(), tag_l.len());
                let event_v = QxBytesStart::owned(tag_v.to_vec(), tag_v.len());

                event_l.push_attribute(("key", validate_name(key)?));

                writer.write_event(QxEvent::Start(event_l))?;
                writer.write_event(QxEvent::Start(event_v))?;
                attributes
                    .iter()
                    .map(|a| a.write_xes(writer))
                    .collect::<Result<()>>()?;
                writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag_v)))?;
                writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag_l)))?;

                return Ok(());
            }
        };

        let tag = tag.as_bytes();
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        event.push_attribute(("key", validate_name(key)?));
        event.push_attribute(("value", value));

        Ok(writer.write_event(QxEvent::Empty(event))?)
    }

    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<()> {
        Self::write_xes_kv(&self.key, &self.value, writer)
    }
}

impl TryFrom<XesIntermediate> for XesValue {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let mut attributes: Vec<Attribute> = Vec::new();

        for component in intermediate.components {
            match component {
                XesComponent::Attribute(attribute) => attributes.push(attribute),
                other => warn!("unexpected child component of value: {:?}, ignore", other),
            }
        }

        Ok(XesValue { attributes })
    }
}

impl TryFrom<XesIntermediate> for ExtensionDecl {
    type Error = Error;

    fn try_from(mut intermediate: XesIntermediate) -> Result<Self> {
        Ok(ExtensionDecl {
            name: intermediate.pop("name")?,
            prefix: intermediate.pop("prefix")?,
            uri: intermediate.pop("uri")?,
        })
    }
}

impl ExtensionDecl {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<()> {
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

        for component in intermediate.components {
            match component {
                XesComponent::Attribute(attribute) => attributes.push(attribute),
                other => warn!("unexpected child component of global: {:?}, ignore", other),
            }
        }

        Ok(Global { scope, attributes })
    }
}

impl Global {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<()> {
        let tag = b"global";
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        match self.scope {
            Scope::Event => event.push_attribute(("scope", "event")),
            Scope::Trace => event.push_attribute(("scope", "trace")),
        }

        writer.write_event(QxEvent::Start(event))?;
        self.attributes
            .iter()
            .map(|a| a.write_xes(writer))
            .collect::<Result<()>>()?;
        writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag)))?;

        Ok(())
    }
}

impl TryFrom<XesIntermediate> for ClassifierDecl {
    type Error = Error;

    fn try_from(mut intermediate: XesIntermediate) -> Result<Self> {
        Ok(ClassifierDecl {
            name: intermediate.pop("name")?,
            scope: Scope::try_from(intermediate.attributes.get("scope").cloned())?,
            keys: intermediate.pop("keys")?,
        })
    }
}

impl ClassifierDecl {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<()> {
        let tag = b"classifier";
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        event.push_attribute(("name", validate_ncname(self.name.as_str())?));
        match self.scope {
            Scope::Event => event.push_attribute(("scope", "event")),
            Scope::Trace => event.push_attribute(("scope", "trace")),
        }
        event.push_attribute(("keys", validate_token(self.keys.as_str())?));

        Ok(writer.write_event(QxEvent::Empty(event))?)
    }
}

impl Meta {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<()> {
        self.extensions
            .iter()
            .map(|e| e.write_xes(writer))
            .collect::<Result<()>>()?;
        self.globals
            .iter()
            .map(|g| g.write_xes(writer))
            .collect::<Result<()>>()?;
        self.classifiers
            .iter()
            .map(|c| c.write_xes(writer))
            .collect::<Result<()>>()?;
        self.attributes
            .iter()
            .map(|(k, v)| Attribute::write_xes_kv(k, v, writer))
            .collect::<Result<()>>()?;

        Ok(())
    }
}

impl TryFrom<XesIntermediate> for Event {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let mut attributes: BTreeMap<String, AttributeValue> = BTreeMap::new();

        for component in intermediate.components {
            match component {
                XesComponent::Attribute(attribute) => {
                    attributes.insert(attribute.key, attribute.value);
                }
                other => warn!("unexpected child component of event: {:?}, ignore", other),
            }
        }

        Ok(Event { attributes })
    }
}

impl Event {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<()> {
        let tag = b"event";
        let event = QxBytesStart::owned(tag.to_vec(), tag.len());

        writer.write_event(QxEvent::Start(event))?;
        self.attributes
            .iter()
            .map(|(k, v)| Attribute::write_xes_kv(k, v, writer))
            .collect::<Result<()>>()?;
        writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag)))?;

        Ok(())
    }
}

impl TryFrom<XesIntermediate> for Trace {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let mut attributes: BTreeMap<String, AttributeValue> = BTreeMap::new();
        let mut traces: Vec<Event> = Vec::new();

        for component in intermediate.components {
            match component {
                XesComponent::Attribute(attribute) => {
                    attributes.insert(attribute.key, attribute.value);
                }
                XesComponent::Event(event) => traces.push(event),
                other => warn!("unexpected child component of trace: {:?}, ignore", other),
            }
        }

        Ok(Trace {
            attributes,
            events: traces,
        })
    }
}

impl Trace {
    fn write_xes<W: io::Write>(&self, writer: &mut QxWriter<W>) -> Result<()> {
        let tag = b"trace";
        let event = QxBytesStart::owned(tag.to_vec(), tag.len());

        writer.write_event(QxEvent::Start(event))?;
        self.attributes
            .iter()
            .map(|(k, v)| Attribute::write_xes_kv(k, v, writer))
            .collect::<Result<()>>()?;
        self.events
            .iter()
            .map(|e| e.write_xes(writer))
            .collect::<Result<()>>()?;
        writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag)))?;

        Ok(())
    }
}

// The following code is in fact useless for now as all log components are emitted individually when
// streaming and are not cached in the intermediate. Hence, the given intermediate is empty
// resulting in an empty log. However, when one decides to implement a non-streaming XES parser,
// the code below may be useful.
impl TryFrom<XesIntermediate> for Log {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let mut meta = Meta::default();
        let mut traces: Vec<Trace> = Vec::new();
        let mut events: Vec<Event> = Vec::new();

        for component in intermediate.components {
            match component {
                XesComponent::ExtensionDecl(extension) => meta.extensions.push(extension),
                XesComponent::Global(global) => meta.globals.push(global),
                XesComponent::ClassifierDecl(classifier) => meta.classifiers.push(classifier),
                XesComponent::Attribute(attribute) => {
                    meta.attributes.insert(attribute.key, attribute.value);
                }
                XesComponent::Trace(trace) => traces.push(trace),
                XesComponent::Event(event) => events.push(event),
                other => warn!("unexpected child component of log: {:?}, ignore", other),
            }
        }

        Ok(Log {
            meta,
            traces,
            events,
        })
    }
}

impl TryFrom<XesIntermediate> for XesComponent {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        match intermediate.type_name.as_str() {
            "string" | "date" | "int" | "float" | "boolean" | "id" | "list" => {
                Ok(XesComponent::Attribute(Attribute::try_from(intermediate)?))
            }
            "values" => Ok(XesComponent::Value(XesValue::try_from(intermediate)?)),
            "extension" => Ok(XesComponent::ExtensionDecl(ExtensionDecl::try_from(
                intermediate,
            )?)),
            "global" => Ok(XesComponent::Global(Global::try_from(intermediate)?)),
            "classifier" => Ok(XesComponent::ClassifierDecl(ClassifierDecl::try_from(
                intermediate,
            )?)),
            "event" => Ok(XesComponent::Event(Event::try_from(intermediate)?)),
            "trace" => Ok(XesComponent::Trace(Trace::try_from(intermediate)?)),
            "log" => Ok(XesComponent::Log(Log::try_from(intermediate)?)),
            other => Err(Error::XesError(format!(
                "unexpected XES component: {:?}",
                other
            ))),
        }
    }
}

#[derive(Debug)]
struct XesIntermediate {
    type_name: String,
    attributes: HashMap<String, String>,
    components: Vec<XesComponent>,
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
            components: Vec::new(),
        })
    }

    fn pop(&mut self, key: &str) -> Result<String> {
        Ok(self.attributes.remove(key).ok_or_else(|| {
            Error::KeyError(format!(
                "missing {:?} attribute in {:?}",
                key, self.type_name
            ))
        })?)
    }

    fn add_component(&mut self, component: XesComponent) {
        self.components.push(component)
    }
}

/// XML deserialization of XES
pub struct XesReader<R: io::BufRead> {
    reader: QxReader<R>,
    buffer: Vec<u8>,
    stack: Vec<XesIntermediate>,
    cache: Option<Component>,
    meta: Option<Meta>,
    empty: bool,
}

impl<R: io::BufRead> XesReader<R> {
    pub fn new(reader: R) -> Self {
        XesReader {
            reader: QxReader::from_reader(reader),
            buffer: Vec::new(),
            stack: Vec::new(),
            cache: None,
            meta: Some(Meta::default()),
            empty: true,
        }
    }
}

impl<R: io::BufRead> From<R> for XesReader<R> {
    fn from(reader: R) -> Self {
        XesReader::new(reader)
    }
}

impl<R: io::BufRead> XesReader<R> {
    fn update(&mut self, intermediate: XesIntermediate) -> ResOpt {
        let component = XesComponent::try_from(intermediate)?;

        if self.stack.len() <= 1 {
            match component {
                XesComponent::ExtensionDecl(extension) => {
                    if let Some(meta) = &mut self.meta {
                        meta.extensions.push(extension);
                    } else {
                        return Err(Error::StateError(format!("unexpected: {:?}", extension)));
                    }
                }
                XesComponent::Global(global) => {
                    if let Some(meta) = &mut self.meta {
                        meta.globals.push(global);
                    } else {
                        return Err(Error::StateError(format!("unexpected: {:?}", global)));
                    }
                }
                XesComponent::ClassifierDecl(classifier) => {
                    if let Some(meta) = &mut self.meta {
                        meta.classifiers.push(classifier)
                    } else {
                        return Err(Error::StateError(format!("unexpected: {:?}", classifier)));
                    }
                }
                XesComponent::Attribute(attribute) => {
                    if let Some(meta) = &mut self.meta {
                        meta.attributes.insert(attribute.key, attribute.value);
                    } else {
                        return Err(Error::StateError(format!("unexpected: {:?}", attribute)));
                    }
                }
                XesComponent::Value(value) => {
                    return Err(Error::StateError(format!("unexpected: {:?}", value)));
                }
                XesComponent::Trace(trace) => {
                    return if let Some(meta) = self.meta.take() {
                        self.cache = Some(Component::Trace(trace));
                        Ok(Some(Component::Meta(meta)))
                    } else {
                        Ok(Some(Component::Trace(trace)))
                    };
                }
                XesComponent::Event(event) => {
                    return if let Some(meta) = self.meta.take() {
                        self.cache = Some(Component::Event(event));
                        Ok(Some(Component::Meta(meta)))
                    } else {
                        Ok(Some(Component::Event(event)))
                    };
                }
                XesComponent::Log(_) => {
                    self.empty = false;
                    if let Some(meta) = self.meta.take() {
                        return Ok(Some(Component::Meta(meta)));
                    }
                }
            }
        } else if let Some(intermediate) = self.stack.last_mut() {
            intermediate.add_component(component);
        }

        Ok(None)
    }
}

impl<T: io::BufRead + Send> Stream for XesReader<T> {
    fn inner_ref(&self) -> Option<&dyn Stream> {
        None
    }

    fn inner_mut(&mut self) -> Option<&mut dyn Stream> {
        None
    }

    fn next(&mut self) -> ResOpt {
        // At the transition of the meta data fields to actual stream data the first trace/event
        // will be cached and emitted in the next iteration. This is supposed to happen once per
        // stream at max.
        if let Some(component) = self.cache.take() {
            return Ok(Some(component));
        }

        loop {
            match self.reader.read_event(&mut self.buffer) {
                Ok(QxEvent::Start(event)) => {
                    let intermediate = XesIntermediate::from_event(event)?;
                    self.stack.push(intermediate);
                }
                Ok(QxEvent::End(_event)) => {
                    let intermediate = self.stack.pop().unwrap();
                    if let Some(component) = self.update(intermediate)? {
                        return Ok(Some(component));
                    }
                }
                Ok(QxEvent::Empty(event)) => {
                    let intermediate = XesIntermediate::from_event(event)?;
                    if let Some(component) = self.update(intermediate)? {
                        return Ok(Some(component));
                    }
                }
                Err(error) => {
                    return Err(Error::XesError(format!(
                        "Error at position {}: {:?}",
                        self.reader.buffer_position(),
                        error
                    )));
                }
                Ok(QxEvent::Eof) => {
                    if self.empty {
                        return Err(Error::XesError(String::from("No root component found")));
                    }
                    break;
                }
                _ => (),
            }

            self.buffer.clear();
        }

        Ok(None)
    }
}

/// XML serialization of XES
pub struct XesWriter<W: io::Write> {
    writer: QxWriter<W>,
}

impl<W: io::Write> XesWriter<W> {
    pub fn new(writer: W, indent_char: Option<u8>, indent_size: Option<usize>) -> Self {
        let writer = QxWriter::new_with_indent(
            writer,
            indent_char.unwrap_or(b'\t'),
            indent_size.unwrap_or(1),
        );

        XesWriter { writer }
    }
}

impl<W: io::Write + Send> StreamSink for XesWriter<W> {
    fn on_open(&mut self) -> Result<()> {
        // XML declaration
        let declaration = QxBytesDecl::new(b"1.0", Some(b"UTF-8"), None);
        self.writer.write_event(QxEvent::Decl(declaration))?;

        // write comments
        [
            &format!(" This file has been generated by promi {} ", crate::VERSION),
            " It conforms to the XML serialization of the XES standard (IEEE Std 1849-2016) ",
            " For log storage and management, see http://www.xes-standard.org. ",
            " promi is available at https://crates.io/crates/promi ",
        ]
            .iter()
            .map(|s| {
                self.writer
                    .write_event(QxEvent::Comment(QxBytesText::from_plain_str(s)))
            })
            .collect::<QxResult<()>>()?;

        // write contents
        let tag = b"log";
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        event.push_attribute(("xes.version", "1849.2016"));
        event.push_attribute(("xes.features", ""));

        self.writer.write_event(QxEvent::Start(event))?;

        Ok(())
    }

    fn on_component(&mut self, component: Component) -> Result<()> {
        match component {
            Component::Meta(meta) => meta.write_xes(&mut self.writer)?,
            Component::Trace(trace) => trace.write_xes(&mut self.writer)?,
            Component::Event(event) => event.write_xes(&mut self.writer)?,
        };

        Ok(())
    }

    fn on_close(&mut self) -> Result<()> {
        let event = QxEvent::End(QxBytesEnd::borrowed(b"log"));

        self.writer.write_event(event)?;
        self.writer.write_event(QxEvent::Eof)?;

        Ok(())
    }
}

impl<W: io::Write> XesWriter<W> {
    /// Get a reference of the underlying writer
    pub fn inner(&mut self) -> &W {
        self.writer.inner()
    }

    /// Release the underlying writer
    pub fn into_inner(self) -> W {
        self.writer.into_inner()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::io::Write;
    use std::path::PathBuf;
    use std::process::{Command, Output, Stdio};

    use crate::dev_util::{expand_static, open_buffered};
    use crate::stream::buffer::Buffer;
    use crate::stream::void::consume;

    use super::*;

    fn deserialize_dir(path: PathBuf, expect_failure: bool) {
        for p in fs::read_dir(path).unwrap().map(|p| p.unwrap()) {
            let f = open_buffered(&p.path());
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
                    format!(
                        "parsing {:?} unexpectedly failed: {:?}",
                        p.path(),
                        result.err()
                    )
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
            .stderr(Stdio::piped())
            .spawn()
            .expect("xmllint installed?");

        child.stdin.as_mut().unwrap().write_all(xes).unwrap();
        child.wait_with_output().unwrap()
    }

    fn validate_dir(path: PathBuf) {
        for p in fs::read_dir(path).unwrap().map(|p| p.unwrap()) {
            let f = open_buffered(&p.path());
            let mut buffer = Buffer::default();

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
            let f = open_buffered(&p.path());
            let mut buffer = Buffer::default();
            let mut snapshots: Vec<Vec<u8>> = Vec::new();

            buffer.consume(&mut XesReader::from(f)).unwrap();

            for _ in 0..2 {
                // serialize to XML
                let bytes: Vec<u8> = Vec::new();
                let mut writer = XesWriter::new(bytes, None, None);
                writer.consume(&mut buffer).unwrap();

                // make snapshot
                let bytes = writer.into_inner();
                snapshots.push(bytes.clone());

                // deserialize from XML
                let mut reader = XesReader::from(io::Cursor::new(bytes));
                buffer
                    .consume(&mut reader)
                    .expect(format!("{:?}", p.path()).as_str());
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
}
