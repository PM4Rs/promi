//! Common promi error type
//!

// standard library
use std::fmt::Debug;

// third party
use thiserror::Error;

// local
use crate::stream;
use std::string::FromUtf8Error;

#[derive(Error, Debug, Clone)]
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
    XMLError(String),

    #[error("cannot parse {0} to boolean")]
    ParseBooleanError(String),

    #[error("{0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("{0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("{0}")]
    FromUtf8Error(String),

    #[error("{0}")]
    ParseDateTimeError(#[from] chrono::ParseError),

    #[error("{0}")]
    XesError(String),
}

// Convert error types that don't support cloning.
impl From<quick_xml::Error> for Error {
    fn from(err: quick_xml::Error) -> Self {
        Error::XMLError(format!("{:?}", err))
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error::FromUtf8Error(format!("{:?}", err))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
