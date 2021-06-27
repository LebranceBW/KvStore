//!
pub use naive_pool::NaiveThreadPool;
pub use shared_pool::SharedQueueThreadPool;

mod naive_pool;
mod shared_pool;


/// empty struct
pub struct RayonThreadPool;

/// Common trait defined for thread pool.
pub trait ThreadPool {
    /// Crate a new instance.
    fn new(threads: u32) -> anyhow::Result<Self>
        where
            Self: Sized;
    /// Create a new thread.
    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + 'static;
}


