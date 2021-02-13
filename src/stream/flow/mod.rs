//! Build (potentially parallel) stream processing pipelines
//!
//! ```
//! use std::path::Path;
//!
//! use promi::Result;
//! use promi::stream::Attribute;
//! use promi::stream::stats::Statistics;
//! use promi::stream::flow::{Segment, FlowGraph, ThreadExecutor};
//!
//!# fn main() -> Result<()> {
//! let path: String = Path::new(env!("CARGO_MANIFEST_DIR"))
//!    .join("static/xes/book/bigger-example.xes").to_str().unwrap().into();
//! let mut pg = FlowGraph::default();
//!
//! pg.source(
//!     "Train",
//!     Segment::new("XesReader")
//!         .attribute(Attribute::new("path", path)))
//!     .stream(Segment::new("Repair"))?
//!     .stream(Segment::new("Validator"))?
//!     .stream(Segment::new("Statistics")
//!         .emit_artifact("raw_stats"))?
//!     .stream(Segment::new("Sample")
//!         .attribute(Attribute::new("ratio", 0.1))
//!         .attribute(Attribute::new("seed", 0)))?
//!     .stream(Segment::new("Statistics")
//!         .emit_artifact("sample_stats"))?
//!     .stream(Segment::new("Split")
//!         .attribute(Attribute::new("ratio", 0.8))
//!         .attribute(Attribute::new("seed", 0))
//!         .emit_stream("test"))?
//!     .stream(Segment::new("Statistics")
//!         .emit_artifact("train_stats"))?
//!     .sink(Segment::new("XesWriter")
//!         .attribute(Attribute::new("path", "/tmp/train.xes"))
//!         .attribute(Attribute::new("indent", 1)))?;
//!
//! pg.source(
//!     "Test",
//!     Segment::new("Receiver")
//!         .acquire_stream("test"))
//!     .stream(Segment::new("Statistics")
//!         .emit_artifact("test_stats"))?
//!     .sink(Segment::new("XesWriter")
//!         .attribute(Attribute::new("path","/tmp/test.xes")))?;
//!
//! pg.execute(ThreadExecutor::default())?;
//!
//! let params = &[
//!     ("raw_stats", [1391, 7539, 7539]),
//!     ("sample_stats", [149, 821, 821]),
//!     ("train_stats", [120, 660, 660]),
//!     ("test_stats", [29, 161, 161]),
//! ];
//!
//! for (name, counts) in params.iter() {
//!     let stats = pg.artifacts.get(*name).unwrap().downcast_ref::<Statistics>().unwrap();
//!     assert_eq!(stats.counts(), *counts);
//! }
//!
//!# Ok(())
//!# }
//! ```
//!
pub use executor::{Executor, SequentialExecutor, ThreadExecutor};
pub use graph::FlowGraph;
pub use segment::Segment;

pub mod executor;
pub mod graph;
pub mod pipe;
pub mod segment;
pub mod util;
