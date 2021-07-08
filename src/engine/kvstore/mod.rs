mod file_operators;
mod kvstore;

use anyhow::Result;
pub use kvstore::KvStore;

use serde::Deserialize;
use serde::Serialize;
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Insertion { key: String, value: String },
    Discard { key: String },
}
