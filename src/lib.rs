#![deny(missing_docs)]
//! KV store

use std::fmt::{Display, Formatter};
use std::str::FromStr;

pub use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

pub use client::KvClient;
pub use engine::{switch_engine, Engine};
pub use kvstore::KvStore;
pub use server::KvServer;
pub use server::ServerConfig;

mod client;
mod engine;
mod kvstore;
mod server;
mod sled;

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
            _ => bail!("Invalid kernel type: {}", s),
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

#[cfg(test)]
mod test {
    use std::thread;

    use anyhow::{Context, Result};
    use mockall::predicate::*;
    use simple_logger::SimpleLogger;

    use crate::client::CommandClient;
    use crate::engine::MockEngine;
    use crate::server::CommandServer;

    use super::Response;
    use super::*;

    fn create_testee() -> Result<(CommandServer, CommandClient)> {
        let addr = "localhost:9999";
        Ok((CommandServer::bind(&addr)?, CommandClient::connect(&addr)?))
    }

    #[test]
    fn test_network_instructions() {
        SimpleLogger::new().init().unwrap();
        let (server, mut client) = create_testee().expect("Failed when creating testee.");
        let _server_thread = thread::spawn(move || {
            server.run(|ins| {
                Response::from(serde_json::to_string(&ins).context("Error when serializing."))
            });
        });
        let sample_data = Instruction::Rm {
            key: "123".to_string(),
        };
        let sample_data2 = Instruction::Get {
            key: "123".to_string(),
        };
        let sample_data3 = Instruction::Set {
            key: "123".to_string(),
            value: "sample".to_string(),
        };
        assert_eq!(
            client.send_instruction(sample_data.clone()).unwrap(),
            serde_json::to_string(&sample_data).unwrap()
        );
        assert_eq!(
            client.send_instruction(sample_data2.clone()).unwrap(),
            serde_json::to_string(&sample_data2).unwrap()
        );
        assert_eq!(
            client.send_instruction(sample_data3.clone()).unwrap(),
            serde_json::to_string(&sample_data3).unwrap()
        );
    }

    #[test]
    fn mock_test() {
        let _server_thread = thread::spawn(move || {
            let mut mocked_engine = Box::new(MockEngine::new());
            mocked_engine
                .expect_get()
                .with(eq("key1"))
                .times(1)
                .returning(|_| Ok(Some("value1".to_owned())));
            mocked_engine
                .expect_remove()
                .with(eq("key2"))
                .return_once(|_| Ok(()));
            let server = KvServer {
                server: CommandServer::bind("localhost:9999").unwrap(),
                engine: mocked_engine,
            };
            server.run()
        });
        let mut client = CommandClient::connect("localhost:9999").unwrap();
        let val = client
            .send_instruction(Instruction::Get {
                key: "key1".to_string(),
            })
            .unwrap();
        let val2 = client.send_instruction(Instruction::Rm {
            key: "key2".to_string(),
        });
        assert_eq!(val, "value1".to_string());
        assert!(val2.is_ok());
    }
}
