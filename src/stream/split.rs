//! Perform a train-test-split on an event stream

// standard library

// third party
use rand::{distributions::Open01, random, Rng};
use rand_pcg::Pcg64;

// local
use crate::stream::{Element, ResOpt, Stream, StreamSink};

/// Train-Test split
///
/// Create a random train-test split of a event stream by a given ratio. Traces and events for
/// training are forwarded, those for testing are sent to a stream sink. This struct may also be
/// used for random sampling only.
///
pub struct Split<T: Stream, S: StreamSink> {
    stream: T,
    test_sink: S,
    train_ratio: f64,
    rng: Pcg64,
}

impl<T: Stream, S: StreamSink> Split<T, S> {
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

impl<T: Stream, S: StreamSink> Stream for Split<T, S> {
    fn get_inner(&self) -> Option<&dyn Stream> {
        Some(&self.stream)
    }

    fn get_inner_mut(&mut self) -> Option<&mut dyn Stream> {
        Some(&mut self.stream)
    }

    fn next(&mut self) -> ResOpt {
        loop {
            match self.stream.next() {
                Ok(Some(Element::Meta(meta))) => {
                    let element = Element::Meta(meta);
                    self.test_sink.on_open()?;
                    self.test_sink.on_element(element.clone())?;
                    return Ok(Some(element));
                }
                Ok(Some(element)) => {
                    let coin: f64 = self.rng.sample(Open01);
                    if coin > self.train_ratio {
                        self.test_sink.on_element(element.clone())?;
                    } else {
                        return Ok(Some(element));
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
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::dev_util::{assert_is_close, load_example};
    use crate::stream::buffer::Buffer;
    use crate::stream::channel::stream_channel;
    use crate::stream::observer::Observer;
    use crate::stream::stats::{Statistics, StatsHandler};
    use crate::stream::{consume, Artifact, Log};

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
                let mut train_counter = Observer::from((split, StatsHandler::default()));

                let artifacts = consume(&mut train_counter).unwrap();

                let [_, train_trace_ct, train_event_ct] =
                    Artifact::find::<Statistics>(artifacts.as_slice())
                        .unwrap()
                        .counts();

                let mut test_counter = Observer::from((test_receiver, StatsHandler::default()));
                let artifacts = consume(&mut test_counter).unwrap();

                let [_, test_trace_ct, test_event_ct] =
                    Artifact::find::<Statistics>(artifacts.as_slice())
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

            assert_is_close(train_trace_ratio, *ratio, Some(1.5 * 1e-2), None, None);
            assert_is_close(train_event_ratio, *ratio, Some(1.5 * 1e-2), None, None);
        }
    }
}
