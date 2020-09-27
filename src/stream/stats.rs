//! Infer statistics from an event stream.
//!
//! # Example
//! This example illustrates how to deserialize XES XML from a string and compute statistics from
//! the event stream.
//! ```
//! use std::io;
//! use promi::stream::{
//!     Artifact,
//!     consume,
//!     observer::Observer,
//!     stats::{StatsCollector, Statistics},
//!     Stream,
//!     StreamSink,
//!     xes::XesReader
//! };
//! use promi::stream::observer::Handler;
//!
//! let s = r#"<?xml version="1.0" encoding="UTF-8"?>
//!            <log xes.version="1.0" xes.features="">
//!                <trace>
//!                    <string key="id" value="Case1.0"/>
//!                    <event>
//!                        <string key="id" value="A"/>
//!                    </event>
//!                    <event>
//!                        <string key="id" value="B"/>
//!                    </event>
//!                </trace>
//!                <event>
//!                    <string key="id" value="C"/>
//!                </event>
//!                <event>
//!                    <string key="id" value="D"/>
//!                </event>
//!            </log>"#;
//!
//! let reader = XesReader::from(io::BufReader::new(s.as_bytes()));
//! let mut stats_collector = StatsCollector::default().into_observer(reader);
//!
//! let artifacts = consume(&mut stats_collector).unwrap();
//!
//! let statistics = Artifact::find::<Statistics>(&artifacts).unwrap();
//!
//! assert_eq!([1, 2, 4], statistics.counts());
//! println!("{}", statistics);
//! ```
//!

use std::fmt;
use std::fmt::Debug;
use std::mem;

use crate::error::Result;
use crate::stream::{observer::Handler, Artifact, Event, Trace};

/// Container for statistical data of an event stream
#[derive(Debug, Clone)]
pub struct Statistics {
    ct_trace: Vec<usize>,
    ct_event: usize,
}

impl Statistics {
    pub fn counts(&self) -> [usize; 3] {
        [
            // number of traces observed
            self.ct_trace.len(),
            // number of events observed in traces
            self.ct_trace.iter().sum::<usize>(),
            // number of events observed in total
            self.ct_event,
        ]
    }
}

impl Default for Statistics {
    fn default() -> Self {
        Statistics {
            ct_trace: Vec::new(),
            ct_event: 0,
        }
    }
}

impl fmt::Display for Statistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sa_events = self.ct_event - self.ct_trace.iter().sum::<usize>();
        writeln!(f, "StreamStats")?;
        writeln!(f, "   traces:              {:?}", self.ct_trace.len())?;
        writeln!(f, "   events:              {:?}", self.ct_event)?;
        writeln!(f, "   events (standalone): {:?}", sa_events)?;
        Ok(())
    }
}

/// Generate statistics from event stream
#[derive(Debug)]
pub struct StatsCollector {
    pub statistics: Statistics,
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self {
            statistics: Statistics::default(),
        }
    }
}

impl Handler for StatsCollector {
    fn on_trace(&mut self, trace: Trace) -> Result<Option<Trace>> {
        self.statistics.ct_trace.push(trace.events.len());
        Ok(Some(trace))
    }

    fn on_event(&mut self, event: Event, _in_trace: bool) -> Result<Option<Event>> {
        self.statistics.ct_event += 1;
        Ok(Some(event))
    }

    fn release_artifacts(&mut self) -> Result<Vec<Artifact>> {
        Ok(vec![Artifact::new(mem::take(&mut self.statistics))])
    }
}

#[cfg(test)]
mod tests {
    use crate::dev_util::load_example;
    use crate::stream::{consume, observer::Observer};

    use super::*;

    #[test]
    fn test_stream_stats() {
        let param = [
            ("book", "L1.xes", [6, 23, 23]),
            ("book", "L2.xes", [13, 80, 80]),
            ("book", "L3.xes", [4, 39, 39]),
            ("book", "L4.xes", [147, 441, 441]),
            ("book", "L5.xes", [14, 92, 92]),
            ("correct", "log_correct_attributes.xes", [0, 0, 0]),
            ("correct", "event_correct_attributes.xes", [1, 2, 4]),
        ];

        for (d, f, e) in param.iter() {
            let buffer = load_example(&[d, f]);
            let mut observer = Observer::new(buffer);
            observer.register(StatsCollector::default());

            let artifacts = consume(&mut observer).unwrap();
            assert_eq!(
                Artifact::find::<Statistics>(artifacts.as_slice())
                    .unwrap()
                    .counts(),
                *e
            );
        }
    }
}
