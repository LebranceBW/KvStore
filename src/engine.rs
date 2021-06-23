use anyhow::Result;

use mockall::mock;

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

pub struct MockKvsEngine;

mock! {
    pub MockKvsEngine {
        fn new() -> Self;
    }
    impl Clone for MockKvsEngine {
        fn clone(&self) -> Self;
    }
    impl KvsEngine for MockKvsEngine {
        fn get(&self, key: &str) -> Result<Option<String>>;
        fn set(&self, key: &str, value: &str) -> Result<()>;
        fn remove(&self, key: &str) -> Result<()>;
    }
}
// Return an anonymous backend engine by EngineType Enum at specific directory.
// pub fn switch_engine<T: Into<PathBuf>>(t: EngineType, path: T) -> Result<Box<dyn KvsEngine>> {
//     match t {
//         EngineType::Kvs => KvStore::open(path).map(|x| Box::new(x) as Box<dyn KvsEngine>),
//         EngineType::Mock => Ok(Box::new(MockKvsEngine::new()) as Box<dyn KvsEngine>),
//         EngineType::Sled => {
//             Box::new(SledAdapter::open(path)).map(|x| Box::new(x) as Box<dyn KvsEngine>)
//         }
//     }
// }
