//! Thread safe and buffered channels
//!

// standard library
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};

// third party

// local
use crate::error::{Error, Result};
use crate::stream::{Element, ResOpt, Stream, StreamSink};

/// Represents the sending endpoint of an asynchronous channel
pub struct StreamSender {
    sender: Sender<ResOpt>,
}

impl StreamSink for StreamSender {
    fn on_element(&mut self, element: Element) -> Result<()> {
        self.sender.send(Ok(Some(element)))?;
        Ok(())
    }

    fn on_close(&mut self) -> Result<()> {
        self.sender.send(Ok(None))?;
        Ok(())
    }

    fn on_error(&mut self, error: Error) -> Result<()> {
        self.sender.send(Err(error))?;
        Ok(())
    }
}

/// Represents the sending endpoint of a synchronous channel
pub struct SyncStreamSender {
    sender: SyncSender<ResOpt>,
}

impl StreamSink for SyncStreamSender {
    fn on_element(&mut self, element: Element) -> Result<()> {
        self.sender.send(Ok(Some(element)))?;
        Ok(())
    }

    fn on_close(&mut self) -> Result<()> {
        self.sender.send(Ok(None))?;
        Ok(())
    }

    fn on_error(&mut self, error: Error) -> Result<()> {
        self.sender.send(Err(error))?;
        Ok(())
    }
}

/// Represents the receiving endpoint of a channel
pub struct StreamReceiver {
    receiver: Receiver<ResOpt>,
}

impl Stream for StreamReceiver {
    fn next(&mut self) -> ResOpt {
        self.receiver.recv()?
    }
}

/// Create a thread safe asynchronous stream channel
///
/// A stream channel is represented by a sender and a receiver, whereby the sender is a stream sink
/// and the receiver a stream. The channel provides a theoretically infinite sized buffer. Hence,
/// sending will never block.
///
pub fn stream_channel() -> (StreamSender, StreamReceiver) {
    let (sender, receiver) = channel();
    (StreamSender { sender }, StreamReceiver { receiver })
}

/// Create a thread safe synchronous stream channel
///
/// A stream channel is represented by a sender and a receiver, whereby the sender is a stream sink
/// and the receiver a stream. The channel provides a buffer of predefined size (0 is okay). Hence,
/// sending will never once the buffer is full.
///
pub fn sync_stream_channel(bound: usize) -> (SyncStreamSender, StreamReceiver) {
    let (sender, receiver) = sync_channel(bound);
    (SyncStreamSender { sender }, StreamReceiver { receiver })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::stats::Counter;
    use crate::stream::xes::XesReader;
    use crate::stream::{consume, Duplicator};
    use crate::util::{expand_static, open_buffered};
    use std::path::PathBuf;
    use std::thread;

    fn _test_channel(path: PathBuf, expect_error: bool) {
        // channels from main thread to helper threads
        let (s_t0_t1, mut r_t0_t1) = stream_channel();
        let (s_t0_t2, mut r_t0_t2) = stream_channel();

        // channels from helper threads back to main thread
        let (mut s_t1_t0, r_t1_t0) = sync_stream_channel(0);
        let (mut s_t2_t0, r_t2_t0) = sync_stream_channel(64);

        // spawn helper threads
        let t_1 = thread::spawn(move || {
            assert_eq!(s_t1_t0.consume(&mut r_t0_t1).is_err(), expect_error);
        });

        let t_2 = thread::spawn(move || {
            assert_eq!(s_t2_t0.consume(&mut r_t0_t2).is_err(), expect_error);
        });

        // pipeline for main thread: file > XesReader > Duplicator > Duplicator > Counter
        let f = open_buffered(&path);
        let reader = XesReader::from(f);
        let d_t1 = Duplicator::new(reader, s_t0_t1);
        let d_t2 = Duplicator::new(d_t1, s_t0_t2);

        let mut c_t0 = Counter::new(d_t2);
        let mut c_t1 = Counter::new(r_t1_t0);
        let mut c_t2 = Counter::new(r_t2_t0);

        // TODO investigate!
        // execute pipeline (order is important!)
        assert_eq!(consume(&mut c_t0).is_err(), expect_error);
        assert_eq!(consume(&mut c_t1).is_err(), expect_error);
        assert_eq!(consume(&mut c_t2).is_err(), expect_error);

        t_1.join().unwrap();
        t_2.join().unwrap();

        assert_eq!(c_t0.counts(), c_t1.counts());
        assert_eq!(c_t0.counts(), c_t2.counts());
    }

    #[test]
    fn test_channel() {
        let param = [
            ("book", "L1.xes"),
            ("book", "L2.xes"),
            ("book", "L3.xes"),
            ("book", "L4.xes"),
            ("book", "L5.xes"),
            ("correct", "log_correct_attributes.xes"),
            ("correct", "event_correct_attributes.xes"),
        ];

        for (d, f) in param.iter() {
            _test_channel(expand_static(&["xes", d, f]), false);
        }

        let param = [
            ("non_parsing", "boolean_incorrect_value.xes"),
            ("non_parsing", "broken_xml.xes"),
            ("non_parsing", "element_incorrect.xes"),
            ("non_parsing", "no_log.xes"),
            ("non_parsing", "global_incorrect_scope.xes"),
        ];

        for (d, f) in param.iter() {
            _test_channel(expand_static(&["xes", d, f]), true);
        }
    }

    #[test]
    fn test_channel_error() {
        let (mut s, r) = stream_channel();
        drop(r);

        assert!(s.on_close().is_err());

        let (mut s, r) = sync_stream_channel(42);
        drop(r);

        assert!(s.on_close().is_err());

        let (s, mut r) = stream_channel();
        drop(s);

        assert!(r.next().is_err());

        let (s, mut r) = sync_stream_channel(0);
        drop(s);

        assert!(r.next().is_err());
    }
}
