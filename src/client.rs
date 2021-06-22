use std::io::{BufRead, BufReader, LineWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

use anyhow::{bail, Context, Result};

use crate::{Instruction, Response};

pub struct CommandClient {
    stream: TcpStream,
}

impl CommandClient {
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(Self { stream })
    }

    pub(crate) fn send_instruction(&mut self, ins: Instruction) -> Result<String> {
        let mut buf_reader = BufReader::new(&self.stream);
        let mut line_writer = LineWriter::new(&self.stream);
        let serialized = serde_json::to_string(&ins)?;
        writeln!(line_writer, "{}", serialized)?;
        let mut buf = String::new();
        buf_reader.read_line(&mut buf)?;
        let resp: Response = serde_json::from_str(buf.trim())
            .with_context(|| format!("Error when parsing from json. {}", buf))?;
        match resp {
            Response::Ok(s) => Ok(s),
            Response::Error(s) => bail!(s),
        }
    }
}

/// KvClient,work for communicating with KvServer.
pub struct KvClient {
    client: CommandClient,
}

impl KvClient {
    /// connect to KvServer listening on `addr`.
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        Ok(KvClient {
            client: CommandClient::connect(addr)?,
        })
    }

    /// Get the value by provided key.
    pub fn get(&mut self, key: String) -> Result<String> {
        self.client.send_instruction(Instruction::Get { key })
    }
    /// Insert a key-value pair.
    pub fn set(&mut self, key: String, value: String) -> Result<String> {
        self.client
            .send_instruction(Instruction::Set { key, value })
    }
    /// Remove an existing key-value pair or report error.
    pub fn remove(&mut self, key: String) -> Result<String> {
        self.client.send_instruction(Instruction::Rm { key })
    }
}
