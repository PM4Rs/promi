//! Useful, potentially panicking functions for developing promi.
//!

extern crate simple_logger;

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, Once};

use log::LevelFilter;
use simple_logger::SimpleLogger;

use crate::stream::buffer::Buffer;
use crate::stream::xes::XesReader;
use crate::stream::{Artifact, ResOpt, Stream, StreamSink};
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

/// Access assets
///
/// For developing promi it's useful to work with some test files that are located in `/static`.
/// In order to locate these in your system, this function exists. It takes a list of relative
/// location descriptors and expands them to an absolute path.
///
pub fn expand_static(path: &[&str]) -> PathBuf {
    let mut exp = Path::new(env!("CARGO_MANIFEST_DIR")).join("static");

    for p in path.iter() {
        exp = exp.join(p);
    }

    exp
}

/// Open a file as `io::BufReader`
pub fn open_buffered(path: &Path) -> io::BufReader<File> {
    io::BufReader::new(File::open(&path).unwrap_or_else(|_| panic!("No such file {:?}", &path)))
}

/// Stream that fails on purpose after any number of components or while emitting artifacts
struct FailingStream<T: Stream> {
    stream: T,
    count: i64,
    fails: i64,
    panic: bool,
}

impl<T: Stream> FailingStream<T> {
    /// Create a new failing stream
    ///
    /// If _fails_ is set to a non negative value the stream will turn into the error state after
    /// this number of components returned or the very last one. In the case _fails_ is negative,
    /// the stream succeeds but fails on emitting artifacts.
    ///
    pub fn new(stream: T, fails: i64, panic: bool) -> Self {
        Self {
            stream,
            count: 0,
            fails,
            panic,
        }
    }
}

impl<T: Stream> Stream for FailingStream<T> {
    fn get_inner(&self) -> Option<&dyn Stream> {
        Some(&self.stream)
    }

    fn get_inner_mut(&mut self) -> Option<&mut dyn Stream> {
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
            (Some(_), true, true) | (None, _, true) => {
                let msg = format!("{}/{}: stream failed on purpose on component", self.count, self.fails);
                if self.panic {
                    panic!(msg);
                } else {
                    Err(Error::StreamError(msg))
                }
            }
        }
    }

    fn on_emit_artifacts(&mut self) -> Result<Vec<Artifact>> {
        let msg = format!("{}/{}: stream failed on purpose on emitting artifacts", self.count, self.fails);
        if self.panic {
            panic!(msg);
        } else {
            Err(Error::ArtifactError(msg))
        }
    }
}

/// Check whether two floats are close to each other
///
/// In many scenario e.g. testing it is often times more useful to know whether two floating point
/// numbers are close than exactly equal as. Due to finite precision of computers, we usually cannot
/// even expect factual equality even if math suggests it. This function is strongly inspired by
/// [Python's PEP 485](https://www.python.org/dev/peps/pep-0485/).
///
/// `rel_tol` is the relative tolerance -- the amount of error allowed, relative to the magnitude of
/// the input values.
///
/// `abs_tol` is the minimum absolute tolerance level -- useful for comparisons to zero.
///
/// Supported methods for comparing `a` abd `b` are:
///
/// - `asymmetric`: the `b` value is used for scaling the tolerance
/// - `average`: the tolerance is scaled by the average of the two values
/// - `strong`: the tolerance is scaled by the smaller of the two values
/// - `weak`: the tolerance is scaled by the larger of the two values (default/fallback)
///
/// **NOTE** if given method is unknown, this function falls back on the `weak` criterion rather
/// than failing!
///
#[allow(clippy::float_cmp)]
pub fn is_close(
    a: f64,
    b: f64,
    rel_tol: Option<f64>,
    abs_tol: Option<f64>,
    method: Option<&str>,
) -> bool {
    // trivial case
    if a == b {
        return true;
    }

    // get tolerance values and ensure they are non negative
    let rel_tol = rel_tol.unwrap_or(1e-8).abs();
    let abs_tol = abs_tol.unwrap_or(0.0).abs();

    // check border cases
    let diff = (b - a).abs();
    if !diff.is_finite() {
        return false;
    }

    // assess difference by chosen method
    match method.unwrap_or("weak") {
        "asymmetric" => (diff <= (rel_tol * b).abs()) || (diff <= abs_tol),
        "average" => (diff <= (rel_tol * (a + b) / 2.0).abs() || (diff <= abs_tol)),
        "strong" => {
            ((diff <= (rel_tol * b).abs()) && (diff <= (rel_tol * a).abs())) || (diff <= abs_tol)
        }
        _ => ((diff <= (rel_tol * b).abs()) || (diff <= (rel_tol * a).abs())) || (diff <= abs_tol),
    }
}

/// Convenience wrapper for `is_close`
pub fn assert_is_close(
    a: f64,
    b: f64,
    rel_tol: Option<f64>,
    abs_tol: Option<f64>,
    method: Option<&str>,
) {
    assert!(
        is_close(a, b, rel_tol, abs_tol, method),
        format!("{} is not close to {}", a, b)
    )
}

/// Convenience wrapper for `!is_close`
pub fn assert_is_not_close(
    a: f64,
    b: f64,
    rel_tol: Option<f64>,
    abs_tol: Option<f64>,
    method: Option<&str>,
) {
    assert!(
        !is_close(a, b, rel_tol, abs_tol, method),
        format!("{} is close to {}", a, b)
    )
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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::stream::consume;

    #[test]
    fn test_logging() {
        logging();
        info!("logging enabled!");
    }

    #[test]
    fn test_failing_stream() {
        for i in 0..10 {
            let mut failing = FailingStream::new(load_example(&["book", "L1.xes"]), i, false);
            match consume(&mut failing) {
                Err(Error::StreamError(_)) => (),
                other => panic!(format!("expected stream error, got {:?}", other))
            }
        }

        let mut failing = FailingStream::new(load_example(&["book", "L1.xes"]), -1, false);
        match consume(&mut failing) {
            Err(Error::ArtifactError(_)) => (),
            other => panic!(format!("expected artifact error, got {:?}", other))
        }
    }

    #[test]
    fn test_is_close() {
        // exact cases
        for (a, b) in &[
            (2.0, 2.0),
            (0.1e200, 0.1e200),
            (1.123e-300, 1.123e-300),
            (0.0, -0.0),
        ] {
            assert_is_close(*a, *b, Some(0.0), Some(0.0), None);
        }

        // relative cases
        for (a, b) in &[
            (1e8, 1e8 + 1.),
            (-1e-8, -1.000000009e-8),
            (1.12345678, 1.12345679),
        ] {
            assert_is_close(*a, *b, Some(1e-8), None, None);
            assert_is_not_close(*a, *b, Some(1e-9), None, None);
        }

        // zero test case
        for (a, b) in &[(1e-9, 0.0), (-1e-9, 0.0), (-1e-150, 0.0)] {
            assert_is_not_close(*a, *b, Some(0.9), None, None);
            assert_is_close(*a, *b, None, Some(1e-8), None);
        }

        // non finite cases
        for (a, b) in &[
            (f64::INFINITY, f64::INFINITY),
            (f64::NEG_INFINITY, f64::NEG_INFINITY),
        ] {
            assert_is_close(*a, *b, None, Some(0.999999999999999), None);
        }

        for (a, b) in &[
            (f64::NAN, f64::NAN),
            (f64::NAN, 1e-100),
            (1e-100, f64::NAN),
            (f64::INFINITY, f64::NAN),
            (f64::NAN, f64::INFINITY),
            (f64::INFINITY, f64::NEG_INFINITY),
            (f64::INFINITY, 1.0),
            (1.0, f64::INFINITY),
        ] {
            assert_is_not_close(*a, *b, None, Some(0.999999999999999), None);
        }

        // other methods
        assert_is_close(9.0, 10.0, Some(0.1), None, Some("asymmetric"));
        assert_is_not_close(10.0, 9.0, None, Some(0.1), Some("asymmetric"));
        assert_is_close(9.0, 10.0, Some(0.1), None, Some("weak"));
        assert_is_close(10.0, 9.0, Some(0.1), None, Some("weak"));
        assert_is_not_close(9.0, 10.0, Some(0.1), None, Some("strong"));
        assert_is_not_close(10.0, 9.0, Some(0.1), None, Some("strong"));
        assert_is_not_close(9.0, 10.0, Some(0.1), None, Some("average"));
        assert_is_not_close(10.0, 9.0, Some(0.1), None, Some("average"));
    }
}
