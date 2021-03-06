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

extern crate chrono;
#[cfg(test)]
#[macro_use]
extern crate is_close;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log as logging;
extern crate quick_xml;
extern crate regex;
extern crate serde;
#[cfg(test)]
extern crate simple_logger;
extern crate thiserror;
extern crate typetag;

pub use error::{Error, Result};

#[cfg(test)]
#[macro_use]
pub mod dev_util;
pub mod error;
pub mod stream;

/// promi's datetime type
pub type DateTime = chrono::DateTime<chrono::FixedOffset>;

/// promi version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
