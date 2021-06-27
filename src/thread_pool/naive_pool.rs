
/// A naive implemention of thread poll.
pub struct NaiveThreadPool;

use super::ThreadPool;
use anyhow::Result;
use std::thread;
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
