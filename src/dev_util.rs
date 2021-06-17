//! Useful, potentially panicking functions for developing promi.
//!
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::panic;
use std::path::Path;
use std::sync::{Mutex, Once};
use std::thread;

use log::LevelFilter;
use simple_logger::SimpleLogger;

use crate::stream::buffer::Buffer;
use crate::stream::xes::XesReader;
use crate::stream::{AnyArtifact, ResOpt, Sink, Stream};
use crate::{Error, Result};

static LOGGER: Once = Once::new();

pub fn logging() {
    LOGGER.call_once(|| {
        SimpleLogger::new()
            .with_level(LevelFilter::Debug)
            .init()
            .unwrap()
    });
}

/// Path to static asset
///
/// For developing promi it's useful to work with some test files that are located in `/static`.
/// In order to locate these in your system, this function exists. It takes a list of relative
/// location descriptors and expands them to an absolute path.
///
#[macro_export]
macro_rules! join_static {
    ($first:expr $(, $further:expr)* $(,)?) => {
        {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("static")
                .join($first)
                $(.join($further))*
        }
    };
}

/// Like [`join_static`] but returns a [`String`]
#[macro_export]
macro_rules! join_static_str {
    ($first:expr $(, $further:expr)* $(,)?) => {
        {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("static")
                .join($first)
                $(.join($further))*
                .to_str()
                .unwrap()
                .into()
        }
    };
}

/// Like [`join_static`] but returns a [`std::io::BufReader`]
#[macro_export]
macro_rules! join_static_reader {
    ($first:expr $(, $further:expr)* $(,)?) => {
        {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("static")
                .join($first)
                $(.join($further))*;

            std::io::BufReader::new(std::fs::File::open(&path)
                .unwrap_or_else(|_| panic!("No such file {:?}", &path)))
        }
    };
}

/// Stream that fails on purpose after any number of components or while emitting artifacts
struct FailingStream<T: Stream> {
    stream: T,
    count: i64,
    fails: i64,
}

impl<T: Stream> FailingStream<T> {
    /// Create a new failing stream
    ///
    /// If _fails_ is set to a non negative value the stream will turn into the error state after
    /// this number of components returned or the very last one. In the case _fails_ is negative,
    /// the stream succeeds but fails on emitting artifacts.
    ///
    pub fn new(stream: T, fails: i64) -> Self {
        Self {
            stream,
            count: 0,
            fails,
        }
    }
}

impl<T: Stream> Stream for FailingStream<T> {
    fn inner_ref(&self) -> Option<&dyn Stream> {
        Some(&self.stream)
    }

    fn inner_mut(&mut self) -> Option<&mut dyn Stream> {
        Some(&mut self.stream)
    }

    fn next(&mut self) -> ResOpt {
        self.count += 1;

        match (
            self.stream.next()?,
            self.count >= self.fails - 1,
            self.fails >= 0,
        ) {
            (Some(next), _, false) | (Some(next), false, true) => Ok(Some(next)),
            (None, _, false) => Ok(None),
            (Some(_), true, true) | (None, _, true) => Err(Error::StreamError(format!(
                "[{}/{}] stream failed on purpose on component",
                self.count, self.fails
            ))),
        }
    }

    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        Err(Error::ArtifactError(format!(
            "[{}/{}] stream failed on purpose on emitting artifacts",
            self.count, self.fails
        )))
    }
}

lazy_static! {
    /// Cache for example event streams
    static ref CACHE: Mutex<HashMap<String, Buffer>> = Mutex::new(HashMap::new());
}

/// Read an example XES file from the `static/xes` directory into a stream buffer
pub fn load_example(path: &[&str]) -> Buffer {
    // build path and infer key
    let mut root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("static")
        .join("xes");

    for p in path {
        root = root.join(p);
    }

    let key = root.to_str().expect("cannot turn path into string");

    // check whether example was loaded before
    let mut cache = CACHE.lock().expect("cannot open cache");
    if !cache.contains_key(key) {
        let file = io::BufReader::new(File::open(&root).unwrap());
        let mut reader = XesReader::from(file);
        let mut buffer = Buffer::default();

        if buffer.consume(&mut reader).is_err() {
            warn!(
                "an error occurred while loading: {:?} - this, however, may be intended",
                &root
            );
        }

        cache.insert(key.to_string(), buffer);
    }

    cache.get(key).unwrap().clone()
}

/// Relax a test case by allowing up to `n` failures
#[allow(clippy::panicking_unwrap)]
pub fn retry_up_to<T>(n: usize, test: T)
where
    T: Fn() + panic::UnwindSafe + panic::RefUnwindSafe,
{
    let mut failures: usize = 0;

    for _ in 0.. {
        let result: thread::Result<()> = panic::catch_unwind(|| test());

        if result.is_ok() {
            return;
        }

        failures += 1;

        if failures >= n {
            result.unwrap();
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::stream::void::consume;

    use super::*;

    #[test]
    fn test_logging() {
        logging();
        info!("logging enabled!");
    }

    #[test]
    fn test_failing_stream() {
        for i in 0..10 {
            let mut failing = FailingStream::new(load_example(&["book", "L1.xes"]), i);
            match consume(&mut failing) {
                Err(Error::StreamError(_)) => (),
                other => panic!("expected stream error, got {:?}", other),
            }
        }

        let mut failing = FailingStream::new(load_example(&["book", "L1.xes"]), -1);
        match consume(&mut failing) {
            Err(Error::ArtifactError(_)) => (),
            other => panic!("expected artifact error, got {:?}", other),
        }
    }

    #[test]
    fn test_pass_m_of_n_success() {
        retry_up_to(1, || ());
    }

    #[test]
    #[should_panic]
    fn test_pass_m_of_n_failure() {
        retry_up_to(3, || panic!("ooops"));
    }
}
