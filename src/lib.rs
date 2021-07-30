#![deny(missing_docs)]
//! KV store

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub use anyhow::Result;
pub use client::KvClient;
pub use engine::KvsEngine;
pub use server::KvServer;

mod client;
pub mod engine;
mod server;
pub mod thread_pool;

/// Backend EngineType
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum EngineType {
    /// kvs
    Kvs,
    /// sled
    Sled,
    /// mock
    Mock,
}

impl Display for EngineType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            EngineType::Kvs => "kvs",
            EngineType::Sled => "sled",
            EngineType::Mock => "mock(debug)",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for EngineType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "kvs" => Ok(EngineType::Kvs),
            "sled" => Ok(EngineType::Sled),
            "mock" => Ok(EngineType::Mock),
            _ => anyhow::bail!("Invalid kernel type: {}", s),
        }
    }
}

impl From<EngineType> for String {
    fn from(t: EngineType) -> Self {
        match t {
            EngineType::Kvs => "kvs",
            EngineType::Sled => "sled",
            EngineType::Mock => "mock",
        }
        .parse()
        .unwrap()
    }
}

/// Instructions send by  KvClient/
#[derive(Serialize, Deserialize, Debug, Clone)]
enum Instruction {
    /// Set a key-value pair.
    Set { key: String, value: String },
    /// Get key.
    Get { key: String },
    /// Remove a specific key.
    Rm { key: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Response {
    Ok(String),
    Error(String),
}

impl From<Result<String>> for Response {
    fn from(res: Result<String>) -> Self {
        match res {
            Ok(x) => Response::Ok(x),
            Err(e) => Response::Error(e.to_string()),
        }
    }
}

impl From<Response> for Result<String, String> {
    fn from(res: Response) -> Self {
        match res {
            Response::Ok(s) => Ok(s),
            Response::Error(s) => Err(s),
        }
    }
}
