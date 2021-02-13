//! Perform a train-test-split on an event stream

use chrono::Utc;
use rand::{distributions::Open01, random, Rng};
use rand_pcg::Pcg64;

use crate::stream::plugin::{Declaration, Entry, Factory, FactoryType, PluginProvider};
use crate::stream::void::Void;
use crate::stream::{AnyArtifact, Component, ResOpt, Sink, Stream};
use crate::Result;

/// Train-Test split
///
/// Create a random train-test split of a event stream by a given ratio. Traces and events for
/// training are forwarded, those for testing are sent to a stream sink. This struct may also be
/// used for random sampling only.
///
pub struct Split<T: Stream, S: Sink> {
    stream: T,
    test_sink: S,
    train_ratio: f64,
    rng: Pcg64,
}

impl<T: Stream, S: Sink> Split<T, S> {
    /// Create a new train-test split
    pub fn new(
        train_stream: T,
        test_sink: S,
        train_ratio: f64,
        random_state: Option<u128>,
    ) -> Self {
        Self {
            stream: train_stream,
            test_sink,
            train_ratio,
            rng: Pcg64::new(random_state.unwrap_or_else(random), 0),
        }
    }

    /// Release both, stream and test sink
    pub fn release(self) -> (T, S) {
        (self.stream, self.test_sink)
    }
}

impl<T: Stream, S: Sink> Stream for Split<T, S> {
    fn inner_ref(&self) -> Option<&dyn Stream> {
        Some(&self.stream)
    }

    fn inner_mut(&mut self) -> Option<&mut dyn Stream> {
        Some(&mut self.stream)
    }

    fn next(&mut self) -> ResOpt {
        loop {
            match self.stream.next() {
                Ok(Some(Component::Meta(meta))) => {
                    let component = Component::Meta(meta);
                    self.test_sink.on_open()?;
                    self.test_sink.on_component(component.clone())?;
                    return Ok(Some(component));
                }
                Ok(Some(component)) => {
                    let coin: f64 = self.rng.sample(Open01);
                    if coin > self.train_ratio {
                        self.test_sink.on_component(component.clone())?;
                    } else {
                        return Ok(Some(component));
                    }
                }
                Ok(None) => {
                    self.test_sink.on_close()?;
                    return Ok(None);
                }
                Err(error) => {
                    self.test_sink.on_error(error.clone())?;
                    return Err(error);
                }
            }
        }
    }

    fn on_emit_artifacts(&mut self) -> Result<Vec<AnyArtifact>> {
        self.test_sink.on_emit_artifacts()
    }
}

impl PluginProvider for Split<Box<dyn Stream>, Box<dyn Sink>> {
    fn entries() -> Vec<Entry>
    where
        Self: Sized,
    {
        vec![
            Entry::new(
                "Split",
                "Split stream into two new ones at random",
                Factory::new(
                    Declaration::default()
                        .stream("inner", "The stream to be split")
                        .sink("sink", "The sink that consumes one part of the stream")
                        .attribute("ratio", "Share of events/traces that are kept")
                        .default_attr("seed", "Optional seed", || {
                            Utc::now().timestamp_nanos().into()
                        }),
                    FactoryType::Stream(Box::new(|parameters| -> Result<Box<dyn Stream>> {
                        Ok(Split::new(
                            parameters.acquire_stream("inner")?,
                            parameters.acquire_sink("sink")?,
                            *parameters.acquire_attribute("ratio")?.try_float()?,
                            match parameters.acquire_attribute("seed") {
                                Ok(s) => Some(*(s.try_int()?) as u128),
                                _ => None,
                            },
                        )
                        .into_boxed())
                    })),
                ),
            ),
            Entry::new(
                "Sample",
                "Sample from a stream",
                Factory::new(
                    Declaration::default()
                        .stream("inner", "The stream to be sampled from")
                        .attribute("ratio", "Share of events/traces that are sampled")
                        .default_attr("seed", "Optional seed", || {
                            Utc::now().timestamp_nanos().into()
                        }),
                    FactoryType::Stream(Box::new(|parameters| -> Result<Box<dyn Stream>> {
                        Ok(Split::new(
                            parameters.acquire_stream("inner")?,
                            Void::default(),
                            *parameters.acquire_attribute("ratio")?.try_float()?,
                            match parameters.acquire_attribute("seed") {
                                Ok(s) => Some(*(s.try_int()?) as u128),
                                _ => None,
                            },
                        )
                        .into_boxed())
                    })),
                ),
            ),
        ]
    }
}

#[cfg(test)]
pub mod tests {
    use crate::dev_util::load_example;
    use crate::stream::buffer::Buffer;
    use crate::stream::channel::stream_channel;
    use crate::stream::log::Log;
    use crate::stream::observer::Handler;
    use crate::stream::stats::{Statistics, StatsCollector};
    use crate::stream::{void::consume, AnyArtifact};

    use super::*;

    #[test]
    fn test_split() {
        let repetitions: u128 = 5;
        let mut buffer = load_example(&["book", "L1.xes"]);

        let mut log = Log::default();
        log.consume(&mut buffer).unwrap();

        // inflate log
        for _ in 0..7 {
            log.traces.extend(log.traces.clone())
        }

        // turn some traces into independent events
        for _ in 0..log.traces.len() / 6 {
            if let Some(trace) = log.traces.pop() {
                log.events.extend(trace.events)
            }
        }

        let buffer: Buffer = log.into();
        for ratio in &[0.0, 0.33, 1.0] {
            let mut train_trace_ratio: f64 = 0.0;
            let mut train_event_ratio: f64 = 0.0;

            for seed in 0..repetitions {
                let (test_sender, test_receiver) = stream_channel(None);

                let split = Split::new(buffer.clone(), test_sender, *ratio, Some(seed));
                let mut train_counter = StatsCollector::default().into_observer(split);

                let artifacts = consume(&mut train_counter).unwrap();

                let [_, train_trace_ct, train_event_ct] =
                    AnyArtifact::find::<Statistics>(&mut artifacts.iter().flatten())
                        .unwrap()
                        .counts();

                let mut test_counter = StatsCollector::default().into_observer(test_receiver);
                let artifacts = consume(&mut test_counter).unwrap();

                let [_, test_trace_ct, test_event_ct] =
                    AnyArtifact::find::<Statistics>(&mut artifacts.iter().flatten())
                        .unwrap()
                        .counts();

                if train_event_ct + test_event_ct > 0 {
                    train_event_ratio +=
                        train_event_ct as f64 / (train_event_ct + test_event_ct) as f64
                }

                if train_trace_ct + test_trace_ct > 0 {
                    train_trace_ratio +=
                        train_trace_ct as f64 / (train_trace_ct + test_trace_ct) as f64
                }
            }

            train_trace_ratio /= repetitions as f64;
            train_event_ratio /= repetitions as f64;

            assert!(is_close!(train_trace_ratio, *ratio, rel_tol = 1.5e-2));
            assert!(is_close!(train_event_ratio, *ratio, rel_tol = 1.5e-2));
        }
    }
}
