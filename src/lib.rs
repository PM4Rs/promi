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

#[macro_use]
extern crate log as logging;
#[macro_use]
extern crate lazy_static;
extern crate chrono;
extern crate quick_xml;
extern crate regex;
extern crate thiserror;

pub mod error;
pub mod stream;

pub use error::{Error, Result};

/// promi's datetime type
pub type DateTime = chrono::DateTime<chrono::FixedOffset>;

/// promi version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Useful functions that may panic and are intended for developing promi.
///
pub mod util {
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};

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
        io::BufReader::new(
            fs::File::open(&path).unwrap_or_else(|_| panic!("No such file {:?}", &path)),
        )
    }
}
