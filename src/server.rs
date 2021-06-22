
use std::io::{BufRead, BufReader, LineWriter, Write};

use std::net::{TcpListener, ToSocketAddrs};



use anyhow::Result;
use log::*;
use serde_json;


use crate::{KvsEngine, Response};

use super::Instruction;

/// KvServer, accept instructions from kvclient and process by kv engine.
pub struct KvServer<T: KvsEngine> {
    pub(crate) server: CommandServer,
    pub(crate) engine: T,
}

// const DATA_DIR: &'static str = "./";
// const LOCK_FILE: &'static str = ".engine_lock";

impl<T: KvsEngine> KvServer<T> {
    /// Construct a new instance through ServerConfig.
    pub fn new(engine: T, address: impl ToSocketAddrs) -> Result<Self> {
        Ok(KvServer {
            server: CommandServer::bind(address)?,
            engine,
        })
    }

    /// Start  receiving instructions from client continuesly..
    pub fn run(self) -> ! {
        let KvServer { server, engine } = self;
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
