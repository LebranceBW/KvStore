//! Different implement of key-value engine.
use anyhow::Result;

pub use kvstore::KvStore;
pub use sled_store::SledAdapter;

mod kvstore;
mod sled_store;

/// Trait which Key-Value storage engine should obey.
pub trait KvsEngine: Clone + Send + 'static {
    /// Get value bind by key.
    fn get(&self, key: &str) -> Result<Option<String>>;
    /// Insert a key-value pair.
    fn set(&self, key: &str, value: &str) -> Result<()>;
    /// Remove an existing key-value pair or report error.
    fn remove(&self, key: &str) -> Result<()>;
    /// Flush all In-mem data into the hard device.
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}
