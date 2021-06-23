//!
use std::thread;

use anyhow::Result;

/// empty struct
pub struct RayonThreadPool;

/// empty struct
pub trait SharedQueueThreadPool {}

/// Common trait defined for thread pool.
pub trait ThreadPool {
    /// Crate a new instance.
    fn new(threads: u32) -> Result<Self>
        where
            Self: Sized;
    /// Create a new thread.
    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + 'static;
}

/// A naive implemention of thread poll.
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(Self)
    }

    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}

