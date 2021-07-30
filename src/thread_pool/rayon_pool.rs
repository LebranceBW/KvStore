use anyhow::Result;
use rayon::{ThreadPool as RayonThreadPool, ThreadPoolBuilder};

use crate::thread_pool::ThreadPool;

///
pub struct RayonAdapterPool {
    pool: RayonThreadPool,
}

impl ThreadPool for RayonAdapterPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(RayonAdapterPool {
            pool: ThreadPoolBuilder::new()
                .num_threads(threads as usize)
                .build()
                .unwrap(),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job)
    }
}
