//! Common promi error type
//!

// standard library
use std::fmt::Debug;

// third party
use thiserror::Error;

// local
use crate::stream;

#[derive(Error, Debug)]
pub enum Error {
    #[error("state order violation: cannot go back into {got:?} when in {current:?}")]
    StateError {
        got: stream::StreamState,
        current: stream::StreamState,
    },

    #[error("Stream Error: {0}")]
    StreamError(String),

    #[error("Validation Error: {0}")]
    ValidationError(String),

    #[error("key error {0} not found")]
    KeyError(String),

    #[error("{0}")]
    XMLError(#[from] quick_xml::Error),

    #[error("cannot parse {0} to boolean")]
    ParseBooleanError(String),

    #[error("{0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("{0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("{0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("{0}")]
    ParseDateTimeError(#[from] chrono::ParseError),

    #[error("{0}")]
    XesError(String)
}

pub type Result<T> = std::result::Result<T, Error>;
