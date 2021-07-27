use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;

pub use kvstore::KvStore;

mod file_operators;
mod kvstore;

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Insertion { key: String, value: String },
    Discard { key: String },
}
