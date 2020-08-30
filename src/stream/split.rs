//! Perform a train-test-split on an event stream

// standard library

// third party
use rand::{distributions::Open01, random, Rng};
use rand_pcg::Pcg64;

// local
use crate::stream::{Element, ResOpt, Stream, StreamSink, WrappingStream};

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
            rng: Pcg64::new(random_state.unwrap_or(random()), 0),
        }
    }

    /// Release both, stream and test sink
    pub fn release(self) -> (T, S) {
        (self.stream, self.test_sink)
    }
}

impl<T: Stream, S: StreamSink> Stream for Split<T, S> {
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

impl<T: Stream, S: StreamSink> WrappingStream<T> for Split<T, S> {
    fn inner(&self) -> &T {
        &self.stream
    }

    fn into_inner(self) -> T {
        self.stream
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::dev_util::assert_is_close;
    use crate::stream::buffer::tests::load_example;
    use crate::stream::buffer::Buffer;
    use crate::stream::stats::Counter;
    use crate::stream::{consume, Log};

    #[test]
    fn test_split() {
        let repetitions: u128 = 5;
        let mut buffer = load_example(&["xes", "book", "bigger-example.xes"]);

        let mut log = Log::default();
        log.consume(&mut buffer).unwrap();

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
                let test_buffer = Buffer::default();
                let split = Split::new(buffer.clone(), test_buffer, *ratio, Some(seed));
                let mut train_counter = Counter::new(split);

                consume(&mut train_counter).unwrap();

                let [_, train_trace_ct, train_event_ct] = train_counter.counts();

                let (_, test_buffer) = train_counter.into_inner().release();

                let mut test_counter = Counter::new(test_buffer);
                consume(&mut test_counter).unwrap();

                let [_, test_trace_ct, test_event_ct] = test_counter.counts();

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

            assert_is_close(train_trace_ratio, *ratio, Some(1e-2), None, None);
            assert_is_close(train_event_ratio, *ratio, Some(1e-2), None, None);
        }
    }
}
