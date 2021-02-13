use std::sync::mpsc::Receiver;

use crate::stream::channel::{ChannelNameSpace, Sender};
use crate::stream::{AnyArtifact, ResOpt};

/// Stream channel name space
pub(in crate::stream::flow) type SCNS = ChannelNameSpace<ResOpt, usize>;

/// Sending endpoint of an artifact channel
pub(in crate::stream::flow) type ArtifactSender = Sender<AnyArtifact>;

/// Receiving endpoint of an artifact channel
pub(in crate::stream::flow) type ArtifactReceiver = Receiver<AnyArtifact>;

/// Artifact channel name space
pub(in crate::stream::flow) type ACNS = ChannelNameSpace<AnyArtifact, usize>;
