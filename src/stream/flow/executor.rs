use std::thread;

use crate::{Error, Result};

/// The executor protocol
pub trait Executor {
    /// Submit jobs for execution
    fn schedule<T, J>(&mut self, jobs: T)
    where
        T: IntoIterator<Item = J>,
        J: FnOnce() + Send + 'static;

    /// Wait for jobs to complete
    fn join(&mut self) -> Result<()>;
}

/// Execute jobs on scheduling directly
pub struct SequentialExecutor;

impl Default for SequentialExecutor {
    fn default() -> Self {
        SequentialExecutor {}
    }
}

impl Executor for SequentialExecutor {
    fn schedule<T, J>(&mut self, jobs: T)
    where
        T: IntoIterator<Item = J>,
        J: FnOnce() + Send + 'static,
    {
        jobs.into_iter().for_each(|job| job());
    }

    fn join(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Launch a thread per job
pub struct ThreadExecutor {
    handles: Vec<thread::JoinHandle<()>>,
}

impl Default for ThreadExecutor {
    fn default() -> Self {
        ThreadExecutor {
            handles: Vec::new(),
        }
    }
}

impl Executor for ThreadExecutor {
    fn schedule<T, J>(&mut self, jobs: T)
    where
        T: IntoIterator<Item = J>,
        J: FnOnce() + Send + 'static,
    {
        self.handles.extend(jobs.into_iter().map(thread::spawn))
    }

    fn join(&mut self) -> Result<()> {
        self.handles.drain(..).try_for_each(|job| {
            job.join()
                .map_err(|e| Error::StreamError(format!("{:?}", e)))
        })
    }
}
