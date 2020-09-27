//! Common promi error type
//!

use std::fmt::Debug;

use thiserror::Error;

/// A common error type for promi
#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("{0}")]
    StateError(String),

    #[error("Stream Error: {0}")]
    StreamError(String),

    #[error("Validation Error: {0}")]
    ValidationError(String),

    #[error("key error, {0} not found")]
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

    #[error("{0}")]
    ChannelError(String),

    #[error("{0}")]
    ExtensionError(String),

    #[error("{0}")]
    AttributeError(String),
}

// Manual conversion as quick-xml errors don't support cloning
impl From<quick_xml::Error> for Error {
    fn from(error: quick_xml::Error) -> Self {
        Error::XMLError(format!("{:?}", error))
    }
}

// Manual conversion as string errors errors don't support cloning
impl From<std::string::FromUtf8Error> for Error {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Error::FromUtf8Error(format!("{:?}", error))
    }
}

// Manual conversion to prevent recursion
impl From<std::sync::mpsc::SendError<crate::stream::ResOpt>> for Error {
    fn from(error: std::sync::mpsc::SendError<crate::stream::ResOpt>) -> Self {
        Error::ChannelError(format!("{:?}", error))
    }
}

impl From<std::sync::mpsc::RecvError> for Error {
    fn from(_: std::sync::mpsc::RecvError) -> Self {
        Error::ChannelError(String::from("channel unexpectedly closed"))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
