//! A static representation of an event stream.
//!

use serde::{Deserialize, Serialize};

use crate::Result;
use crate::stream::{Component, Event, Meta, StreamSink, Trace, Attributes, AttributeValue, ComponentType};
use crate::stream::buffer::Buffer;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    pub meta: Meta,
    pub traces: Vec<Trace>,
    pub events: Vec<Event>,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            meta: Meta::default(),
            traces: Vec::new(),
            events: Vec::new(),
        }
    }
}

impl Into<Buffer> for Log {
    fn into(self) -> Buffer {
        let mut buffer = Buffer::default();

        buffer.push(Ok(Some(Component::Meta(self.meta))));

        for trace in self.traces {
            buffer.push(Ok(Some(Component::Trace(trace))));
        }

        for event in self.events {
            buffer.push(Ok(Some(Component::Event(event))));
        }

        buffer
    }
}

impl Attributes for Log {
    fn get(&self, key: &str) -> Option<&AttributeValue> {
        self.meta.attributes.get(key)
    }

    fn children<'a>(&'a self) -> Vec<&'a dyn Attributes> {
        self.traces
            .iter()
            .map(|t| t as &'a dyn Attributes)
            .chain(self.events.iter().map(|e| e as &'a dyn Attributes))
            .collect()
    }

    fn hint(&self) -> ComponentType {
        ComponentType::Meta
    }
}

impl StreamSink for Log {
    fn on_component(&mut self, component: Component) -> Result<()> {
        match component {
            Component::Meta(meta) => self.meta = meta,
            Component::Trace(trace) => self.traces.push(trace),
            Component::Event(event) => self.events.push(event),
        };

        Ok(())
    }
}