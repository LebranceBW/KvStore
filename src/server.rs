use std::io::{BufRead, BufReader, LineWriter, Write};
use std::net::{TcpListener, ToSocketAddrs};

use anyhow::Result;
use log::*;
use serde_json;

use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, Response};

use super::Instruction;

/// KvServer, accept instructions from kvclient and process by kv engine.
pub struct KvServer<T: KvsEngine, K: ThreadPool> {
    pub(crate) server: TcpListener,
    pub(crate) engine: T,
    pool: K,
}

impl<T: KvsEngine, K: ThreadPool> KvServer<T, K> {
    /// Construct a new instance through ServerConfig.
    pub fn new(engine: T, pool: K, address: impl ToSocketAddrs) -> Result<Self> {
        Ok(KvServer {
            server: TcpListener::bind(address)?,
            engine,
            pool,
        })
    }

    fn process_instruction(engine: &mut T, inst: &Instruction) -> Result<Response> {
        Ok(Response::from({
            debug!("command: {:?}", inst);
            let ret = match inst {
                Instruction::Get { key } => engine
                    .get(&key)
                    .map(|x| x.unwrap_or(format!("Key: {} not found", key))),
                Instruction::Set { key, value } => engine.set(&key, &value).map(|_| "".to_owned()),
                Instruction::Rm { key } => engine.remove(&key).map(|_| "".to_owned()),
            };
            engine.flush().unwrap();
            ret
        }))
    }

    /// Start  receiving instructions from client continuesly..
    pub fn run(self) -> ! {
        loop {
            let (stream, client_addr) = self.server.accept().unwrap();
            info!("Accept connection from client: {:?}", client_addr);
            {
                let mut engine = self.engine.clone();
                self.pool.spawn(move || {
                    let buf_reader = BufReader::new(&stream);
                    let mut line_writer = LineWriter::new(&stream);
                    for line in buf_reader.lines() {
                        let line = line.unwrap();
                        debug!("[client->server] {}", line);
                        let ins = serde_json::from_str::<Instruction>(&line).unwrap();
                        let resp = Self::process_instruction(&mut engine, &ins).unwrap();
                        debug!("[server->client] {:?}", resp);
                        let serialized = serde_json::to_string(&resp)
                            .unwrap_or("Failed to serialize response.".to_string());
                        writeln!(&mut line_writer, "{}", serialized).unwrap();
                    }
                });
            }
            info!("Client: {:?} disconnected", client_addr);
        }
    }
}
