use std::collections::HashMap;

/// In memory key-value storage.
///
/// Key-value entry stores in `HashMap` temporarily.
///
///  Example:
/// ```rust
///# use kvs::KvStore;
///let mut store = KvStore::new();
///store.set("key1".to_owned(), "value1".to_owned());
///assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
/// ```

#[derive(Default, Debug)]
pub struct KvStore {
    m: HashMap<String, String>,
}

impl KvStore {
    /// Initialize a new storage.
    pub fn new() -> KvStore {
        Self { m: HashMap::new() }
    }
    /// Get the value by key.
    pub fn get(&self, key: String) -> Option<String> {
        self.m.get(&key).cloned()
    }
    /// Set the value belonging to the provided key.
    pub fn set(&mut self, key: String, val: String) {
        self.m.insert(key, val);
    }
    /// Remove an key whether it exists or not.
    pub fn remove(&mut self, key: String) {
        self.m.remove(&key);
    }
}
