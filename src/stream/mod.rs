//! Extensible event streams
//!
//! This module provides a protocol for event streaming. Its design is strongly inspired by the
//! XES standard\[1\] (see also [xes-standard.org](http://www.xes-standard.org)).
//!
//! Further, it comes with commonly used convenience features such as buffering, filtering, basic
//! statistics, validation and (de-)serialization. The provided API allows you to easily build and
//! customize your data processing pipeline.
//!
//! \[1\] [_IEEE Standard for eXtensible Event Stream (XES) for Achieving Interoperability in Event
//! Logs and Event Streams_, 1849:2016, 2016](https://standards.ieee.org/standard/1849-2016.html)
//!

pub use self::core::artifact::*;
pub use self::core::attribute::*;
pub use self::core::component::*;
pub use self::core::sink::*;
pub use self::core::stream::*;
#[cfg(test)]
pub use self::core::tests;

pub mod core;
// modules
pub mod buffer;
pub mod channel;
pub mod duplicator;
pub mod extension;
pub mod filter;
pub mod log;
pub mod observer;
pub mod plugin;
pub mod repair;
pub mod split;
pub mod stats;
pub mod validator;
pub mod void;
pub mod xes;
pub mod xml_util;
