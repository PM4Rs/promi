use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::mpsc::channel;

use serde::{Deserialize, Serialize};

use crate::stream::flow::pipe::Pipe;
use crate::stream::flow::pipe::PreparedPipe;
use crate::stream::flow::segment::Segment;
use crate::stream::flow::util::{timeit, toposort, ACNS, SCNS};
use crate::stream::flow::Executor;
use crate::stream::AnyArtifact;
use crate::{Error, Result};

/// Directed, acyclic event stream processing graph
#[derive(Debug, Serialize, Deserialize)]
pub struct Graph {
    generation: usize,
    pub artifacts: HashMap<String, AnyArtifact>,
    pub staging: Option<Pipe>,
    pub pipes: Vec<Pipe>,
}

impl Default for Graph {
    fn default() -> Self {
        Graph {
            generation: 0,
            artifacts: HashMap::new(),
            staging: None,
            pipes: Vec::new(),
        }
    }
}

impl Graph {
    /// Add a new source segment
    ///
    /// If there's an open pipe, it is closed and a new one with this source is set staging.
    ///
    pub fn source<N: Into<String>>(&mut self, name: N, source: Segment) -> &mut Self {
        self.close();
        self.staging = Some(Pipe::new(name.into(), source));
        self
    }

    /// Add intermediate stream segment
    ///
    /// If a pipe is staging, the stream is added to it. Otherwise, an error occurs.
    ///
    pub fn stream(&mut self, stream: Segment) -> Result<&mut Self> {
        match &mut self.staging {
            Some(pipe) => {
                pipe.stream(stream);
                Ok(self)
            }
            None => Err(Error::FlowError(
                "nothing is staging, call `source` first".to_string(),
            )),
        }
    }

    /// Add a sink segment
    ///
    /// If a pipe is staging, the stream is added to it and the pipe is closed. Otherwise, an error
    /// occurs.
    ///
    pub fn sink(&mut self, sink: Segment) -> Result<&mut Self> {
        match &mut self.staging {
            Some(pipe) => {
                pipe.sink(sink);
                Ok(())
            }
            None => Err(Error::FlowError(
                "nothing is staging, call `source` first".to_string(),
            )),
        }?;

        self.close();

        Ok(self)
    }

    fn close(&mut self) {
        if let Some(pipe) = self.staging.take() {
            self.pipes.push(pipe);
        }
    }

    /// Build and execute pipes
    ///
    /// A number of things happen when the flow graph is executed:
    /// 1. Pipes register stream/artifact acquisitions/emissions
    /// 2. A dependency graph is built and checked for potential deadlocks
    /// 3. Each pipe is turned into a job which is then scheduled for execution at the given executor
    /// 4. After execution, artifacts are collected and the internal state is updated respectively
    ///
    pub fn execute<E: Executor>(&mut self, executor: &mut E) -> Result<&mut Self> {
        self.close();

        let mut scns = SCNS::default();
        let mut acns = ACNS::default();
        let mut pipes: HashMap<usize, PreparedPipe> = HashMap::new();
        let mut artifacts: HashMap<_, _> = HashMap::new();

        // store a copy of current configuration
        artifacts.insert(
            format!("__PIPES_GEN_{}__", &self.generation),
            AnyArtifact::from(self.pipes.clone()),
        );

        // prepare pipes, i.e. acquire artifacts and streams
        for (generation, pipe) in (1..).zip(self.pipes.drain(..)) {
            scns.set_generation(generation);
            acns.set_generation(generation);
            pipes.insert(generation, pipe.acquire(&mut scns, &mut acns)?);
        }

        // collect remaining endpoints from partially acquired channels
        // artifact channels
        acns.set_generation(0);
        let artifact_senders: HashMap<_, _> = acns.acquire_remaining_senders()?.collect();
        info!("acquire artifact senders: {:?}", artifact_senders.keys());

        acns.set_generation(usize::MAX);
        let artifact_receivers: HashMap<_, _> = acns.acquire_remaining_receivers()?.collect();
        info!(
            "acquire artifact receivers: {:?}",
            artifact_receivers.keys()
        );

        // stream channels
        scns.set_generation(0);
        let stream_senders: HashMap<_, _> = scns.acquire_remaining_senders()?.collect();
        info!("acquire stream senders: {:?}", stream_senders.keys());

        scns.set_generation(usize::MAX);
        let stream_receivers: HashMap<_, _> = scns.acquire_remaining_receivers()?.collect();
        info!("acquire stream receivers: {:?}", stream_receivers.keys());

        // extract dependencies
        let dependencies: HashSet<_> = scns
            .dependencies()?
            .into_iter()
            .chain(acns.dependencies()?)
            .collect();
        info!("pipe dependencies: {:?}", &dependencies);

        // compute schedule and check for deadlocks
        let ordering = toposort(dependencies)?;
        let mut schedule: Vec<_> = pipes.keys().copied().collect();
        schedule.sort_by_key(|i| ordering.iter().position(|j| j == i).unwrap_or(usize::MAX));
        schedule.reverse();

        // provide jobs with a channel endpoint to send back results
        let (result_sender, result_receiver) =
            channel::<(String, Result<Vec<(String, AnyArtifact)>>)>();

        // schedule jobs
        info!("prepare {} jobs", schedule.len());
        let mut jobs = Vec::new();
        for (i, generation) in schedule.iter().enumerate() {
            let pipe = pipes.remove(generation).ok_or_else(|| {
                Error::FlowError(format!(
                    "There's no pipe associated with generation {}",
                    generation
                ))
            })?;

            debug!("  {}. {} ({})", i + 1, &pipe.name, &generation);
            let name = pipe.name.clone();
            let local_sender = result_sender.clone();

            // create actual job
            jobs.push(move || {
                let (duration, _) = timeit(|| {
                    local_sender
                        .send((name.clone(), pipe.execute()))
                        .unwrap_or_else(|_| error!("{:?}: unable to send back results", name));
                });
                info!("pipe {:?} terminates after {:.2?}", name, duration)
            })
        }

        // as long as there's a copy of the sender the receiver will block, thus we drop it explicitly
        drop(result_sender);

        info!("send {} artifacts out to jobs", artifact_senders.len());
        for (name, sender) in artifact_senders {
            debug!("  send: {}", &name);
            let artifact = self.artifacts.remove(&name).expect(&name);
            sender
                .send(artifact)
                .map_err(|_| Error::FlowError(format!("unable to send {:}", name)))?;
        }

        info!("start execution of {} jobs", jobs.len());
        executor.schedule(jobs);

        info!("wait for all jobs to terminate");
        executor.join()?;

        info!("collect anonymous artifacts");
        while let Ok((t_name, result)) = result_receiver.recv() {
            debug!("{}: {:?}", t_name, result);
            for (key, artifact) in result? {
                artifacts.insert(key, artifact);
            }
        }

        info!("collect {} named artifacts", artifact_receivers.len());
        for (name, receiver) in artifact_receivers {
            debug!("  receive: {}", &name);
            artifacts.insert(
                name.clone(),
                receiver
                    .recv()
                    .map_err(|_| Error::FlowError(format!("unable to receive {:?}", name)))?,
            );
        }

        // apply changes now that execution succeeded
        self.generation += 1;
        self.artifacts.extend(artifacts.into_iter());
        Ok(self)
    }
}
