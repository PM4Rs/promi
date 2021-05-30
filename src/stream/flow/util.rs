use std::sync::mpsc::Receiver;

use crate::stream::channel::{ChannelNameSpace, Sender};
use crate::stream::{AnyArtifact, ResOpt};

/// Sending endpoint of an artifact channel
pub(in crate::stream::flow) type ArtifactSender = Sender<AnyArtifact>;

/// Receiving endpoint of an artifact channel
pub(in crate::stream::flow) type ArtifactReceiver = Receiver<AnyArtifact>;

/// Stream channel name space
#[allow(clippy::upper_case_acronyms)]
pub(in crate::stream::flow) type SCNS = ChannelNameSpace<ResOpt, usize>;

/// Artifact channel name space
#[allow(clippy::upper_case_acronyms)]
pub(in crate::stream::flow) type ACNS = ChannelNameSpace<AnyArtifact, usize>;
