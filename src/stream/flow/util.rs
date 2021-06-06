use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use petgraph::algo::toposort as pg_toposort;
use petgraph::prelude::DiGraph;

use crate::stream::channel::{ChannelNameSpace, Sender};
use crate::stream::{AnyArtifact, ResOpt};
use crate::{Error, Result};

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

/// Measure execution time of given closure
pub(in crate::stream::flow) fn timeit<T, F>(function: F) -> (Duration, T)
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = function();
    (Instant::now() - start, result)
}

pub(in crate::stream::flow) fn toposort<T, I>(edges: I) -> Result<Vec<T>>
where
    T: Eq + Hash + Debug + Copy,
    I: IntoIterator<Item = (T, T)>,
{
    let mut graph = DiGraph::<T, ()>::new();
    let mut indeces = HashMap::new();

    for (r, s) in edges {
        indeces.entry(r).or_insert_with(|| graph.add_node(r));
        indeces.entry(s).or_insert_with(|| graph.add_node(s));

        match (indeces.get(&r), indeces.get(&s)) {
            (Some(e_r), Some(e_s)) => {
                graph.add_edge(*e_r, *e_s, ());
            }
            _ => unreachable!(),
        }
    }

    match pg_toposort(&graph, None) {
        Ok(indices) => Ok(indices.into_iter().map(|i| graph[i]).collect::<Vec<_>>()),
        Err(_) => Err(Error::FlowError(
            "unable to perform topological sorting as the graph is not cycle free".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use super::*;

    #[test]
    fn test_timeit() {
        let (duration, _) = timeit(|| sleep(Duration::from_secs_f32(1e-3)));
        assert!(is_close!(duration.as_secs_f32(), 1e-3, abs_tol = 1e-4));

        let (duration, _) = timeit(|| sleep(Duration::from_secs_f32(1e-2)));
        assert!(is_close!(duration.as_secs_f32(), 1e-2, abs_tol = 1e-3));

        let (duration, value) = timeit(|| 42);
        assert!(is_close!(duration.as_secs_f32(), 0., abs_tol = 1e-5));
        assert_eq!(value, 42);
    }

    #[test]
    fn test_topsort() {
        let ordering: Vec<i32> = toposort(vec![]).unwrap();
        assert_eq!(ordering, [0; 0]);

        let ordering = toposort(vec![(3, 4), (2, 4), (1, 2), (1, 3), (2, 3)]).unwrap();
        assert_eq!(ordering, [1, 2, 3, 4]);

        assert!(toposort(vec![(1, 2), (2, 1)]).is_err());
        assert!(toposort(vec![(1, 2), (3, 4), (4, 3)]).is_err());
    }
}
