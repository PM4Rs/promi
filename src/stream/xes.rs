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
//! use promi::stream::Sink;
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
//! let mut writer = xes::XesWriter::with_indent(io::stdout(), b'1', 1);
//!
//! writer.consume(&mut reader).unwrap();
//! ```
//!

use std::collections::{HashMap, VecDeque};
use std::convert::{From, TryFrom};
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use quick_xml::events::{
    BytesDecl as QxBytesDecl, BytesEnd as QxBytesEnd, BytesStart as QxBytesStart,
    BytesText as QxBytesText, Event as QxEvent,
};
use quick_xml::{Reader as QxReader, Writer as QxWriter};

use crate::stream::log::Log;
use crate::stream::plugin::{Declaration, Entry, Factory, FactoryType, PluginProvider};
use crate::stream::xml_util::{
    parse_bool, validate_name, validate_ncname, validate_token, validate_uri,
};
use crate::stream::{
    Attribute, AttributeMap, AttributeValue, ClassifierDecl, Component, Event, ExtensionDecl,
    Global, Meta, ResOpt, Scope, Sink, Stream, Trace,
};
use crate::{DateTime, Error, Result};

#[derive(Debug)]
enum XesComponent {
    Attribute(Attribute),
    Values(XesValue),
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
        let mut children = Vec::new();

        let value = match intermediate.type_name.as_str() {
            "string" => AttributeValue::String(val_str?),
            "date" => AttributeValue::Date(DateTime::parse_from_rfc3339(val_str?.as_str())?),
            "int" => AttributeValue::Int(val_str?.parse::<i64>()?),
            "float" => AttributeValue::Float(val_str?.parse::<f64>()?),
            "boolean" => AttributeValue::Boolean(parse_bool(&val_str?.as_str())?),
            "id" => AttributeValue::Id(val_str?),
            "list" => {
                let mut attributes: Vec<Attribute> = Vec::new();

                for component in intermediate.components.drain(..) {
                    match component {
                        XesComponent::Values(value) => attributes.extend(value.attributes),
                        XesComponent::Attribute(attribute) => children.push(attribute),
                        other => {
                            return Err(Error::XesError(format!(
                                r#""unexpected XES component: {:?}"#,
                                other
                            )))
                        }
                    }
                }

                AttributeValue::List(attributes)
            }
            attr_key => return Err(Error::KeyError(format!("unknown attribute {}", attr_key))),
        };

        for component in intermediate.components.drain(..) {
            match component {
                XesComponent::Attribute(attribute) => children.push(attribute),
                other => {
                    return Err(Error::XesError(format!(
                        r#""unexpected XES component: {:?}"#,
                        other
                    )))
                }
            }
        }

        Ok(if children.is_empty() {
            Attribute::new(key_str, value)
        } else {
            Attribute::with_children(key_str, value, children)
        })
    }
}

impl Attribute {
    fn components_as_events<'a>(
        key: &'a str,
        value: &'a AttributeValue,
        children: &'a [Attribute],
    ) -> Result<Vec<QxEvent<'a>>> {
        let temp_string: String;
        let mut events: VecDeque<QxEvent> = VecDeque::new();

        for child in children.iter() {
            events.extend(child.as_events()?);
        }

        let (tag, value) = match &value {
            AttributeValue::String(value) => ("string", Some(value.as_str())),
            AttributeValue::Date(value) => {
                temp_string = value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);
                ("date", Some(temp_string.as_str()))
            }
            AttributeValue::Int(value) => {
                temp_string = value.to_string();
                ("int", Some(temp_string.as_str()))
            }
            AttributeValue::Float(value) => {
                temp_string = value.to_string();
                ("float", Some(temp_string.as_str()))
            }
            AttributeValue::Boolean(value) => {
                ("boolean", Some(if *value { "true" } else { "false" }))
            }
            AttributeValue::Id(value) => ("id", Some(value.as_str())),
            AttributeValue::List(attributes) => {
                let tag_v = b"values";
                let event_v = QxBytesStart::owned(tag_v.to_vec(), tag_v.len());

                events.push_back(QxEvent::Start(event_v));

                for attribute in attributes {
                    events.extend(attribute.as_events()?)
                }

                events.push_back(QxEvent::End(QxBytesEnd::borrowed(tag_v)));

                ("list", None)
            }
        };

        let tag = tag.as_bytes();
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        event.push_attribute(("key", validate_name(&key)?));

        if let Some(v) = value {
            event.push_attribute(("value", v))
        }

        if events.is_empty() {
            events.push_front(QxEvent::Empty(event));
        } else {
            events.push_front(QxEvent::Start(event));
            events.push_back(QxEvent::End(QxBytesEnd::borrowed(tag)));
        }

        Ok(Vec::from(events))
    }

    fn as_events(&self) -> Result<Vec<QxEvent>> {
        Self::components_as_events(&self.key, &self.value, &self.children)
    }

    fn components_write_xes<'a, W>(
        key: &'a str,
        value: &'a AttributeValue,
        children: &'a [Attribute],
        writer: &mut QxWriter<W>,
    ) -> Result<()>
    where
        W: io::Write,
    {
        Self::components_as_events(key, value, children)?
            .into_iter()
            .try_for_each(|e| writer.write_event(e))
            .map_err(|e| e.into())
    }

    fn write_xes<W>(&self, writer: &mut QxWriter<W>) -> Result<()>
    where
        W: io::Write,
    {
        self.as_events()
            .into_iter()
            .flatten()
            .try_for_each(|e| writer.write_event(e).map_err(|e| e.into()))
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
    fn write_xes<W>(&self, writer: &mut QxWriter<W>) -> Result<()>
    where
        W: io::Write,
    {
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
    fn write_xes<W>(&self, writer: &mut QxWriter<W>) -> Result<()>
    where
        W: io::Write,
    {
        let tag = b"global";
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        match self.scope {
            Scope::Event => event.push_attribute(("scope", "event")),
            Scope::Trace => event.push_attribute(("scope", "trace")),
        }

        writer.write_event(QxEvent::Start(event))?;
        self.attributes
            .iter()
            .try_for_each(|a| a.write_xes(writer))?;
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
    fn write_xes<W>(&self, writer: &mut QxWriter<W>) -> Result<()>
    where
        W: io::Write,
    {
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
    fn write_xes<W>(&self, writer: &mut QxWriter<W>) -> Result<()>
    where
        W: io::Write,
    {
        self.extensions
            .iter()
            .try_for_each(|e| e.write_xes(writer))?;
        self.globals.iter().try_for_each(|g| g.write_xes(writer))?;
        self.classifiers
            .iter()
            .try_for_each(|c| c.write_xes(writer))?;
        self.attributes
            .iter()
            .try_for_each(|(k, v, c)| Attribute::components_write_xes(k, v, c, writer))?;

        Ok(())
    }
}

impl TryFrom<XesIntermediate> for Event {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let mut attributes = AttributeMap::new();

        for component in intermediate.components {
            match component {
                XesComponent::Attribute(attribute) => {
                    attributes.insert(attribute);
                }
                other => warn!("unexpected child component of event: {:?}, ignore", other),
            }
        }

        Ok(Event { attributes })
    }
}

impl Event {
    fn write_xes<W>(&self, writer: &mut QxWriter<W>) -> Result<()>
    where
        W: io::Write,
    {
        let tag = b"event";
        let event = QxBytesStart::owned(tag.to_vec(), tag.len());

        writer.write_event(QxEvent::Start(event))?;
        self.attributes
            .iter()
            .try_for_each(|(k, v, c)| Attribute::components_write_xes(k, v, c, writer))?;
        writer.write_event(QxEvent::End(QxBytesEnd::borrowed(tag)))?;

        Ok(())
    }
}

impl TryFrom<XesIntermediate> for Trace {
    type Error = Error;

    fn try_from(intermediate: XesIntermediate) -> Result<Self> {
        let mut attributes = AttributeMap::new();
        let mut traces: Vec<Event> = Vec::new();

        for component in intermediate.components {
            match component {
                XesComponent::Attribute(attribute) => {
                    attributes.insert(attribute);
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
    fn write_xes<W>(&self, writer: &mut QxWriter<W>) -> Result<()>
    where
        W: io::Write,
    {
        let tag = b"trace";
        let event = QxBytesStart::owned(tag.to_vec(), tag.len());

        writer.write_event(QxEvent::Start(event))?;
        self.attributes
            .iter()
            .try_for_each(|(k, v, c)| Attribute::components_write_xes(k, v, c, writer))?;
        self.events.iter().try_for_each(|e| e.write_xes(writer))?;
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
                    meta.attributes.insert(attribute);
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
            "values" => Ok(XesComponent::Values(XesValue::try_from(intermediate)?)),
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
        self.attributes.remove(key).ok_or_else(|| {
            Error::KeyError(format!(
                "missing {:?} attribute in {:?}",
                key, self.type_name
            ))
        })
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
                        meta.attributes.insert(attribute);
                    } else {
                        return Err(Error::StateError(format!("unexpected: {:?}", attribute)));
                    }
                }
                XesComponent::Values(value) => {
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
    pub fn new(writer: W) -> Self {
        XesWriter {
            writer: QxWriter::new(writer),
        }
    }

    pub fn with_indent(writer: W, indent_char: u8, indent_size: usize) -> Self {
        XesWriter {
            writer: QxWriter::new_with_indent(writer, indent_char, indent_size),
        }
    }
}

impl<W: io::Write + Send> Sink for XesWriter<W> {
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
        .try_for_each(|s| {
            self.writer
                .write_event(QxEvent::Comment(QxBytesText::from_plain_str(s)))
        })?;

        // write contents
        let tag = b"log";
        let mut event = QxBytesStart::owned(tag.to_vec(), tag.len());

        event.push_attribute(("xes.version", "1849.2016"));
        event.push_attribute(("xes.features", "nested-attributes"));

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

/// Dummy struct for XES Plugins
pub struct XesPluginProvider;

impl PluginProvider for XesPluginProvider {
    fn entries() -> Vec<Entry>
    where
        Self: Sized,
    {
        vec![
            Entry::new(
                "XesReader",
                "Parse the XES format from a file",
                Factory::new(
                    Declaration::default().attribute("path", "Location of the XES file"),
                    FactoryType::Stream(Box::new(|parameters| -> Result<Box<dyn Stream>> {
                        let path = parameters
                            .acquire_attribute("path")?
                            .value
                            .try_string()?
                            .to_string();
                        let file = File::open(&Path::new(&path))
                            .map_err(|e| Error::StreamError(format!("{:?}", e)))?;
                        let reader = BufReader::new(file);
                        Ok(XesReader::from(reader).into_boxed())
                    })),
                ),
            ),
            Entry::new(
                "XesWriter",
                "Render the stream into the XES format",
                Factory::new(
                    Declaration::default()
                        .attribute("path", "Location of the XES file")
                        .default_attr("indent", "Indentation", |n| (n, 0).into()),
                    FactoryType::Sink(Box::new(|parameters| -> Result<Box<dyn Sink>> {
                        let path = parameters
                            .acquire_attribute("path")?
                            .value
                            .try_string()?
                            .to_string();
                        let file = File::create(&Path::new(&path))
                            .map_err(|e| Error::StreamError(format!("{:?}", e)))?;
                        let writer = BufWriter::new(file);
                        let indent = parameters
                            .acquire_attribute("indent")?
                            .value
                            .try_int()
                            .map(|v| *v as usize)?;
                        Ok(Box::new(if indent > 0 {
                            XesWriter::with_indent(writer, b'\t', indent)
                        } else {
                            XesWriter::new(writer)
                        }))
                    })),
                ),
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::io::Write;
    use std::path::PathBuf;
    use std::process::{Command, Output, Stdio};

    use crate::stream::buffer::Buffer;
    use crate::stream::void::consume;

    use super::*;

    fn deserialize_directory(path: PathBuf, expect_failure: bool) {
        for p in fs::read_dir(path).unwrap().map(|p| p.unwrap()) {
            let mut reader = XesReader::from(join_static_reader!(&p.path()));
            let result = consume(&mut reader);

            if expect_failure {
                assert!(
                    result.is_err(),
                    "parsing {:?} is expected to fail but didn't",
                    p.path()
                );
            } else {
                assert!(
                    result.is_ok(),
                    "parsing {:?} unexpectedly failed: {:?}",
                    p.path(),
                    result.err()
                );
            }
        }
    }

    // Parse files that comply with the standard.
    #[test]
    fn test_deserialize_correct_files() {
        deserialize_directory(join_static!("xes", "correct"), false);
    }

    // Parse files that technically don't comply with the standard but can be parsed safely.
    #[test]
    fn test_deserialize_recoverable_files() {
        deserialize_directory(join_static!("xes", "recoverable"), false);
    }

    // Parse incorrect files, expecting Failure.
    #[test]
    fn test_deserialize_non_parsing_files() {
        deserialize_directory(join_static!("xes", "non_parsing"), true);
    }

    // Import incorrect files that parse successfully. Most of these error classes can be caught by
    // XesValidator.
    #[test]
    fn test_non_validating_files() {
        deserialize_directory(join_static!("xes", "non_validating"), false);
    }

    fn validate_xes(xes: &[u8]) -> Output {
        let mut child = Command::new("xmllint")
            .arg("--noout")
            .arg("--schema")
            .arg(join_static!("xes", "xes-ieee-1849-2016.xsd"))
            .arg("-")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("xmllint installed?");
        child.stdin.as_mut().unwrap().write_all(xes).unwrap();
        child.wait_with_output().unwrap()
    }

    fn validate_directory(path: PathBuf) {
        for p in fs::read_dir(path).unwrap().map(|p| p.unwrap()) {
            let mut buffer = Buffer::default();
            let mut reader = XesReader::from(join_static_reader!(&p.path()));

            buffer
                .consume(&mut reader)
                .unwrap_or_else(|_| panic!("unable to parse {:?}", p.path()));

            // serialize to XML
            let bytes: Vec<u8> = Vec::new();
            let mut writer = XesWriter::with_indent(bytes, b'\t', 1);
            writer.consume(&mut buffer).unwrap();

            let validation_result = validate_xes(&writer.into_inner()[..]);

            assert!(
                validation_result.status.success(),
                "validation failed for {:?}, {:?}",
                p,
                validation_result
            );
        }
    }

    // Test whether serialization to XES XML representation yield syntactically correct results.
    // This test requires `xmllint` to be available in path.
    #[test]
    fn test_serialize_syntax() {
        validate_directory(join_static!("xes", "correct"));
        validate_directory(join_static!("xes", "recoverable"));
    }

    // deserialize-serialize-deserialize-serialize XES, check whether outputs are consistent
    fn serialize_deserialize_identity(path: PathBuf) {
        for p in fs::read_dir(path).unwrap().map(|p| p.unwrap()) {
            let mut buffer = Buffer::default();
            let mut snapshots: Vec<Vec<u8>> = Vec::new();

            buffer
                .consume(&mut XesReader::from(join_static_reader!(&p.path())))
                .unwrap();

            for _ in 0..2 {
                // serialize to XML
                let bytes: Vec<u8> = Vec::new();
                let mut writer = XesWriter::with_indent(bytes, b'1', 1);
                writer.consume(&mut buffer).unwrap();

                // make snapshot
                let bytes = writer.into_inner();
                snapshots.push(bytes.clone());

                // deserialize from XML
                let mut reader = XesReader::from(io::Cursor::new(bytes));
                buffer
                    .consume(&mut reader)
                    .unwrap_or_else(|_| panic!("{:?}", p.path()));
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
        serialize_deserialize_identity(join_static!("xes", "correct"));
        serialize_deserialize_identity(join_static!("xes", "recoverable"));
    }
}
