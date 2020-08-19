//! Useful functions for developing promi that may panic.
//!

// standard library
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

// third party

// local

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
pub fn open_buffered(path: &Path) -> io::BufReader<fs::File> {
    io::BufReader::new(fs::File::open(&path).unwrap_or_else(|_| panic!("No such file {:?}", &path)))
}
