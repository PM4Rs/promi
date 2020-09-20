//! Thread safe and buffered channels
//!

// standard library
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};

// third party

// local
use crate::error::{Error, Result};
use crate::stream::{Element, ResOpt, Stream, StreamSink};

trait ChannelSender {
    fn send_res_opt(&mut self, element: ResOpt) -> Result<()>;
}

impl ChannelSender for Sender<ResOpt> {
    fn send_res_opt(&mut self, element: ResOpt) -> Result<()> {
        self.send(element)?;
        Ok(())
    }
}

impl ChannelSender for SyncSender<ResOpt> {
    fn send_res_opt(&mut self, element: ResOpt) -> Result<()> {
        self.send(element)?;
        Ok(())
    }
}

/// Represents the sending endpoint of a (synchronous) channel
pub struct StreamSender {
    sender: Box<dyn ChannelSender + Send>,
}

impl StreamSink for StreamSender {
    fn on_element(&mut self, element: Element) -> Result<()> {
        self.sender.send_res_opt(Ok(Some(element)))?;
        Ok(())
    }

    fn on_close(&mut self) -> Result<()> {
        self.sender.send_res_opt(Ok(None))?;
        Ok(())
    }

    fn on_error(&mut self, error: Error) -> Result<()> {
        self.sender.send_res_opt(Err(error))?;
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

/// A stream sender-receiver pair
pub type StreamChannel = (StreamSender, StreamReceiver);

/// Create a thread safe (a)synchronous stream channel
///
/// A stream channel is represented by a sender and a receiver, whereby the sender is a stream sink
/// and the receiver a stream. If the bound chosen is not none, the channel will provide a
/// theoretically infinite sized buffer. Hence, sending will never block.
///
pub fn stream_channel(bound: Option<usize>) -> StreamChannel {
    match bound {
        Some(bound) => {
            let (sender, receiver) = sync_channel(bound);
            (StreamSender { sender: Box::new(sender) }, StreamReceiver { receiver })
        },
        None => {
            let (sender, receiver) = channel();
            (StreamSender { sender: Box::new(sender) }, StreamReceiver { receiver })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_util::{expand_static, open_buffered};
    use crate::stream::{consume, duplicator::Duplicator, stats::Counter, xes::XesReader};
    use std::path::PathBuf;
    use std::thread;

    /// Sets up the following scenario:
    ///
    /// The main thread parses a XES file and computes some statistics based on the event stream.
    /// While doing that, the stream is duplicated twice and sent to two helper threads that do
    /// nothing but sending the stream back to the main thread. Then, the main thread also computes
    /// statistics of those duplicated streams and compares them to the original one.
    ///
    fn _test_channel(path: PathBuf, expect_error: bool) {
        // channels from main thread to helper threads
        let (s_t0_t1, mut r_t0_t1) = stream_channel(None);
        let (s_t0_t2, mut r_t0_t2) = stream_channel(None);

        // channels from helper threads back to main thread
        let (mut s_t1_t0, r_t1_t0) = stream_channel(Some(0));
        let (mut s_t2_t0, r_t2_t0) = stream_channel(Some(64));

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
        let (mut s, r) = stream_channel(None);
        drop(r);

        assert!(s.on_close().is_err());

        let (mut s, r) = stream_channel(Some(42));
        drop(r);

        assert!(s.on_close().is_err());

        let (s, mut r) = stream_channel(None);
        drop(s);

        assert!(r.next().is_err());

        let (s, mut r) = stream_channel(Some(0));
        drop(s);

        assert!(r.next().is_err());
    }
}
