//! Thread safe channels to enable secure, concurrent communication
//!

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::mem;
use std::sync::mpsc::{
    channel as async_channel, sync_channel, Receiver, Sender as AsyncSender, SyncSender,
};

use crate::error::{Error, Result};
use crate::stream::plugin::{Declaration, Factory, FactoryType, Plugin, RegistryEntry};
use crate::stream::{Component, ResOpt, Sink, Stream};

trait ChannelSender<T> {
    fn send_t(&self, t: T) -> Result<()>;
}

impl<T: Send> ChannelSender<T> for AsyncSender<T> {
    fn send_t(&self, t: T) -> Result<()> {
        self.send(t)
            .map_err(|_| Error::ChannelError("unable to send item".to_string()))?;
        Ok(())
    }
}

impl<T: Send> ChannelSender<T> for SyncSender<T> {
    fn send_t(&self, t: T) -> Result<()> {
        self.send(t)
            .map_err(|_| Error::ChannelError("unable to send item".to_string()))?;
        Ok(())
    }
}

/// Container for (a)synchronous sender
pub struct Sender<T> {
    sender: Box<dyn ChannelSender<T> + Send>,
}

impl<T> Sender<T> {
    /// Send item to channel
    pub fn send(&self, t: T) -> Result<()> {
        self.sender.send_t(t)
    }
}

/// Generic sender-receiver pair
pub type Channel<T> = (Sender<T>, Receiver<T>);

/// Create generic, optionally synchronous channel (if bound is set)
pub fn channel<T: Send + 'static>(bound: Option<usize>) -> Channel<T> {
    match bound {
        Some(bound) => {
            let (sender, receiver) = sync_channel(bound);
            (
                Sender {
                    sender: Box::new(sender),
                },
                receiver,
            )
        }
        None => {
            let (sender, receiver) = async_channel();
            (
                Sender {
                    sender: Box::new(sender),
                },
                receiver,
            )
        }
    }
}

/// Represents the sending endpoint of a (synchronous) stream channel
pub type StreamSender = Sender<ResOpt>;

impl Plugin for StreamSender {
    fn entries() -> Vec<RegistryEntry>
    where
        Self: Sized,
    {
        vec![RegistryEntry::new(
            "Sender",
            "Sending stream channel endpoint",
            Factory::new(
                Declaration::default().sink("emit", "The sending sink"),
                FactoryType::Sink(Box::new(|parameters| -> Result<Box<dyn Sink>> {
                    parameters.acquire_sink("emit")
                })),
            ),
        )]
    }
}

impl Sink for StreamSender {
    fn on_component(&mut self, component: Component) -> Result<()> {
        self.sender.send_t(Ok(Some(component)))?;
        Ok(())
    }

    fn on_close(&mut self) -> Result<()> {
        self.sender.send_t(Ok(None))?;
        Ok(())
    }

    fn on_error(&mut self, error: Error) -> Result<()> {
        self.sender.send_t(Err(error))?;
        Ok(())
    }
}

/// Represents the receiving endpoint of a channel
pub type StreamReceiver = Receiver<ResOpt>;

impl Plugin for StreamReceiver {
    fn entries() -> Vec<RegistryEntry>
    where
        Self: Sized,
    {
        vec![RegistryEntry::new(
            "Receiver",
            "Receiving stream channel endpoint",
            Factory::new(
                Declaration::default().stream("acquire", "The stream to be received"),
                FactoryType::Stream(Box::new(|parameters| -> Result<Box<dyn Stream>> {
                    parameters.acquire_stream("acquire")
                })),
            ),
        )]
    }
}

impl Stream for StreamReceiver {
    fn inner_ref(&self) -> Option<&dyn Stream> {
        None
    }

    fn inner_mut(&mut self) -> Option<&mut dyn Stream> {
        None
    }

    fn next(&mut self) -> ResOpt {
        self.recv()?
    }
}

/// A stream sender-receiver pair
pub type StreamChannel = Channel<ResOpt>;

/// Create a thread safe (a)synchronous stream channel
///
/// A stream channel is represented by a sender and a receiver, whereby the sender is a stream sink
/// and the receiver a stream. If the bound chosen is not none, the channel will provide a
/// theoretically infinite sized buffer. Hence, sending will never block.
///
pub fn stream_channel(bound: Option<usize>) -> StreamChannel {
    channel(bound)
}

enum NameSpaceEntry<T, G> {
    Entry(T),
    Generation(G),
}

impl<T, G: Debug> Debug for NameSpaceEntry<T, G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct("Entry");

        match self {
            NameSpaceEntry::Entry(_) => f.field("Entry", &"..."),
            NameSpaceEntry::Generation(g) => f.field("Generation", g),
        };

        f.finish()
    }
}

impl<T, G> NameSpaceEntry<T, G> {
    fn take(&mut self, accessed: G) -> Result<T> {
        let mut new_entry = NameSpaceEntry::Generation(accessed);
        mem::swap(self, &mut new_entry);

        if let NameSpaceEntry::Entry(entry) = new_entry {
            Ok(entry)
        } else {
            mem::swap(self, &mut new_entry);
            Err(Error::ChannelError(
                "entry was already acquired".to_string(),
            ))
        }
    }
}

type SenderEntry<T, G> = NameSpaceEntry<Sender<T>, G>;
type ReceiverEntry<T, G> = NameSpaceEntry<Receiver<T>, G>;
type ChannelEntry<T, G> = (SenderEntry<T, G>, ReceiverEntry<T, G>);
type ChannelMap<T, G> = HashMap<String, ChannelEntry<T, G>>;

/// Manage channels via keys
///
/// Sometimes the respective counter endpoint of a channel is not yet known in the moment of its
/// creation. A channel namespace let's one acquire channel endpoints via a key and keeps the other
/// end for later acquisition. Further, it keeps track of the access order and generations. The
/// latter are incremented manually and help to distinguish acquiring entities.
///
#[derive(Debug)]
pub struct ChannelNameSpace<T, G: Copy> {
    bound: Option<usize>,
    generation: Option<G>,
    channels: ChannelMap<T, G>,
}

impl<T, G: Copy> ChannelNameSpace<T, G> {
    /// Create a synchronous channel namespace
    pub fn sync(bound: usize) -> Self {
        Self {
            bound: Some(bound),
            generation: None,
            channels: HashMap::new(),
        }
    }
}

impl<T, G: Copy> Default for ChannelNameSpace<T, G> {
    fn default() -> Self {
        Self {
            bound: None,
            generation: None,
            channels: HashMap::new(),
        }
    }
}

impl<T: Send + 'static, G: Copy + Eq + Hash> ChannelNameSpace<T, G> {
    fn lookup(&mut self, key: &str) -> Result<&mut ChannelEntry<T, G>> {
        if !self.channels.contains_key(key) {
            let (s, r) = channel(self.bound);
            self.channels.insert(
                key.to_string(),
                (NameSpaceEntry::Entry(s), NameSpaceEntry::Entry(r)),
            );
        }

        match self.channels.get_mut(key) {
            Some(channel) => Ok(channel),
            None => unreachable!(),
        }
    }

    /// Acquire the sender for the given key if possible
    pub fn acquire_sender(&mut self, key: &str) -> Result<Sender<T>> {
        let generation = self
            .generation
            .ok_or_else(|| Error::ChannelError("no generation set".into()))?;
        let (entry, _) = self.lookup(key)?;

        Ok(entry
            .take(generation)
            .map_err(|_| Error::ChannelError(format!("sender {:?} was already acquired", key)))?)
    }

    /// Acquire an iterator over all remaining senders
    pub fn acquire_remaining_senders(
        &mut self,
    ) -> Result<impl Iterator<Item = (String, Sender<T>)>> {
        let generation = self
            .generation
            .ok_or_else(|| Error::ChannelError("no generation set".into()))?;

        let mut senders = Vec::new();
        for (key, (entry, _)) in self.channels.iter_mut() {
            if let Ok(sender) = entry.take(generation) {
                senders.push((key.clone(), sender))
            }
        }

        Ok(senders.into_iter())
    }

    /// Acquire the receiver for the given key if possible
    pub fn acquire_receiver(&mut self, key: &str) -> Result<Receiver<T>> {
        let generation = self
            .generation
            .ok_or_else(|| Error::ChannelError("no generation set".into()))?;
        let (_, entry) = self.lookup(key)?;

        Ok(entry
            .take(generation)
            .map_err(|_| Error::ChannelError(format!("receiver {:?} was already acquired", key)))?)
    }

    /// Acquire an iterator over all remaining receivers
    pub fn acquire_remaining_receivers(
        &mut self,
    ) -> Result<impl Iterator<Item = (String, Receiver<T>)>> {
        let generation = self
            .generation
            .ok_or_else(|| Error::ChannelError("no generation set".into()))?;

        let mut receivers = Vec::new();
        for (key, (_, entry)) in self.channels.iter_mut() {
            if let Ok(receiver) = entry.take(generation) {
                receivers.push((key.clone(), receiver))
            }
        }

        Ok(receivers.into_iter())
    }

    /// Increment generation
    pub fn set_generation(&mut self, generation: G) {
        self.generation = Some(generation);
    }

    /// Get current generation
    pub fn generation(&self) -> Option<G> {
        self.generation
    }

    /// Compute inter-generation dependencies
    ///
    /// Compute inter-generation dependencies i.e. tuples of receiver generation and sender
    /// generation. This is useful for detecting circular dependencies. Endpoints that have not been
    /// acquired yet cause an error.
    ///
    pub fn dependencies(&self) -> Result<HashSet<(G, G)>> {
        let mut dependencies = HashSet::new();

        for (key, channel) in self.channels.iter() {
            match channel {
                (NameSpaceEntry::Entry(_), NameSpaceEntry::Generation(_)) => {
                    return Err(Error::ChannelError(format!(
                        "sender {:} was not acquired",
                        key
                    )))
                }
                (NameSpaceEntry::Generation(_), NameSpaceEntry::Entry(_)) => {
                    return Err(Error::ChannelError(format!(
                        "receiver {:} was not acquired",
                        key
                    )))
                }
                (NameSpaceEntry::Generation(a_sg), NameSpaceEntry::Generation(a_rg)) => {
                    dependencies.insert((*a_rg, *a_sg));
                }
                _ => (),
            }
        }

        Ok(dependencies)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::thread;

    use crate::dev_util::{expand_static, open_buffered};
    use crate::stream::observer::Handler;
    use crate::stream::stats::{Statistics, StatsCollector};
    use crate::stream::{duplicator::Duplicator, void::consume, xes::XesReader, AnyArtifact};

    use super::*;

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

        let mut c_t0 = StatsCollector::default().into_observer(d_t2);
        let mut c_t1 = StatsCollector::default().into_observer(r_t1_t0);
        let mut c_t2 = StatsCollector::default().into_observer(r_t2_t0);

        // execute pipeline (order is important!)
        let mut results = Vec::new();
        match consume(&mut c_t0) {
            Ok(artifacts) => results.push(
                AnyArtifact::find::<Statistics>(&mut artifacts.iter().flatten())
                    .unwrap()
                    .counts(),
            ),
            _ => assert!(expect_error),
        }
        match consume(&mut c_t1) {
            Ok(artifacts) => results.push(
                AnyArtifact::find::<Statistics>(&mut artifacts.iter().flatten())
                    .unwrap()
                    .counts(),
            ),
            _ => assert!(expect_error),
        }
        match consume(&mut c_t2) {
            Ok(artifacts) => results.push(
                AnyArtifact::find::<Statistics>(&mut artifacts.iter().flatten())
                    .unwrap()
                    .counts(),
            ),
            _ => assert!(expect_error),
        }

        // wait for threads to complete
        t_1.join().unwrap();
        t_2.join().unwrap();

        for w in results.as_slice().windows(2) {
            match w {
                [c1, c2] => assert_eq!(c1, c2),
                _ => unimplemented!(),
            }
        }
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

    #[test]
    fn test_channel_name_space() {
        let mut cns = ChannelNameSpace::<usize, usize>::default();

        assert!(cns.acquire_sender("foo").is_err());

        cns.set_generation(0);
        let s_1 = cns.acquire_sender("foo").unwrap();
        let r_1 = cns.acquire_receiver("foo").unwrap();

        s_1.send(13).unwrap();
        assert_eq!(r_1.recv().unwrap(), 13);

        assert!(cns.acquire_sender("foo").is_err());
        assert!(cns.acquire_receiver("foo").is_err());

        cns.set_generation(1);
        let s_2 = cns.acquire_sender("bar").unwrap();
        cns.set_generation(2);
        let r_2 = cns.acquire_receiver("bar").unwrap();

        s_2.send(37).unwrap();
        assert_eq!(r_2.recv().unwrap(), 37);

        assert!(cns.acquire_sender("bar").is_err());
        assert!(cns.acquire_receiver("bar").is_err());

        cns.set_generation(3);
        let _ = cns.acquire_sender("fnord").unwrap();

        assert!(cns.dependencies().is_err());

        let _ = cns.acquire_receiver("fnord").unwrap();
        let dependencies: HashSet<(usize, usize)> =
            vec![(0, 0), (2, 1), (3, 3)].into_iter().collect();

        assert_eq!(dependencies, cns.dependencies().unwrap());

        let mut cns = ChannelNameSpace::<usize, usize>::default();
        cns.set_generation(0);

        let sender = cns.acquire_sender("foo").unwrap();
        let receiver = cns.acquire_receiver("bar").unwrap();

        sender.send(42).unwrap();

        for (name, s) in cns.acquire_remaining_senders().unwrap() {
            assert_eq!(name, "bar");
            s.send(1337).unwrap();
        }

        for (name, r) in cns.acquire_remaining_receivers().unwrap() {
            assert_eq!(name, "foo");
            assert_eq!(r.recv().unwrap(), 42);
        }

        assert_eq!(receiver.recv().unwrap(), 1337);
    }
}
