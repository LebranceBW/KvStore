use std::path::PathBuf;

use anyhow::Result;
use mockall::automock;

use crate::EngineType;
use crate::kvstore::KvStore;
use crate::sled::SledAdapter;

/// Trait which Key-Value storage engine should obey.
#[automock]
pub trait Engine {
    /// Get value bind by key.
    fn get(&mut self, key: &str) -> Result<Option<String>>;
    /// Insert a key-value pair.
    fn set(&mut self, key: &str, value: &str) -> Result<()>;
    /// Remove an existing key-value pair or report error.
    fn remove(&mut self, key: &str) -> Result<()>;
    /// Flush all In-mem data into the hard device.
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Return an anonymous backend engine by EngineType Enum at specific directory.
pub fn switch_engine<T: Into<PathBuf>>(t: EngineType, path: T) -> Result<Box<dyn Engine>> {
    match t {
        EngineType::Kvs => KvStore::open(path).map(|x| Box::new(x) as Box<dyn Engine>),
        EngineType::Mock => Ok(Box::new(MockEngine::new()) as Box<dyn Engine>),
        EngineType::Sled => {
            Box::new(SledAdapter::open(path)).map(|x| Box::new(x) as Box<dyn Engine>)
        }
    }
}
