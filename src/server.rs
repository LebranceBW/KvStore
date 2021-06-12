use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, LineWriter, Read, Write};
use std::net::{TcpListener, ToSocketAddrs};
use std::net::SocketAddrV4;
use std::str::FromStr;

use anyhow::bail;
use anyhow::Result;
use log::*;
use serde_json;
use structopt::*;

use crate::{Engine, EngineType, Response, switch_engine};

use super::Instruction;

/// KVServer configuration.
#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server", version = env ! ("CARGO_PKG_VERSION"))]
pub struct ServerConfig {
    #[structopt(short = "a", long = "addr", default_value = "127.0.0.1:4000")]
    pub(crate) address: SocketAddrV4,
    #[structopt(short = "t", long = "engine", default_value = "kvs")]
    pub(crate) engine_type: EngineType,
}

/// KvServer, accept instructions from kvclient and process by kv engine.
pub struct KvServer {
    pub(crate) server: CommandServer,
    pub(crate) engine: Box<dyn Engine>,
}

const DATA_DIR: &'static str = "./";
const LOCK_FILE: &'static str = ".engine_lock";

impl KvServer {
    /// Construct a new instance through ServerConfig.
    pub fn new(config: ServerConfig) -> Result<KvServer> {
        let mut lock_fp = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(format!("{}{}", DATA_DIR, LOCK_FILE));
        let prev_engine = match &mut lock_fp {
            Ok(fp) => {
                let mut buf = String::new();
                fp.read_to_string(&mut buf)?;
                EngineType::from_str(&buf).ok()
            }
            _ => None,
        };
        match prev_engine {
            Some(prev) => {
                info!("Retrieving last work. engine: {}", prev);
                if prev != config.engine_type {
                    error!(
                        "Mismatched engine type!, previous engine: {}, new engine: {}",
                        prev, config.engine_type
                    );
                    bail!(
                        "Mismatched engine type!, previous engine: {}, new engine: {}",
                        prev,
                        config.engine_type
                    )
                }
            }
            None => {
                write!(lock_fp?, "{}", String::from(config.engine_type))?;
            }
        }
        info!(
            "Listened at {}, powered by {}, version: {}",
            config.address,
            config.engine_type,
            env!("CARGO_PKG_VERSION")
        );
        Ok(KvServer {
            server: CommandServer::bind(config.address)?,
            engine: switch_engine(config.engine_type, DATA_DIR)?,
        })
    }

    /// Start  receiving instructions from client continuesly..
    pub fn run(self) -> ! {
        let KvServer { server, mut engine } = self;
        server.run(move |ins| {
            Response::from({
                debug!("command: {:?}", ins);
                let ret = match ins {
                    Instruction::Get { key } => engine
                        .get(&key)
                        .map(|x| x.unwrap_or(format!("Key: {} not found", key))),
                    Instruction::Set { key, value } => {
                        engine.set(&key, &value).map(|_| "".to_owned())
                    }
                    Instruction::Rm { key } => engine.remove(&key).map(|_| "".to_owned()),
                };
                engine.flush().unwrap();
                ret
            })
        })
    }
}

pub(crate) struct CommandServer {
    server: TcpListener,
}

impl<'a> CommandServer {
    pub fn bind(addr: impl ToSocketAddrs) -> Result<Self> {
        Ok(Self {
            server: TcpListener::bind(addr)?,
        })
    }

    pub fn run<T>(&self, mut op: T) -> !
        where
            T: FnMut(Instruction) -> Response,
    {
        loop {
            let (stream, client_addr) = self.server.accept().unwrap();
            info!("Accept connection from client: {:?}", client_addr);
            let buf_reader = BufReader::new(&stream);
            let mut line_writer = LineWriter::new(&stream);
            for line in buf_reader.lines() {
                let line = line.unwrap();
                debug!("[client->server] {}", line);
                let ins = serde_json::from_str::<Instruction>(&line).unwrap();
                let resp = op(ins);
                debug!("[server->client] {:?}", resp);
                let serialized = serde_json::to_string(&resp)
                    .unwrap_or("Failed to serialize response.".to_string());
                writeln!(&mut line_writer, "{}", serialized).unwrap();
            }
            info!("Client: {:?} disconnected", client_addr);
        }
    }
}
