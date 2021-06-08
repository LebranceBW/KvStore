use std::collections::HashMap;

use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use serde::Deserialize;
use serde::Serialize;


use crate::Result;

const FILE_PATH: &'static str = "storage.txt";

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
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Insertion { key: String, value: String },
    Discard { key: String },
}

type CommandIndex = u64;
// #[derive(Default, Debug)]
// struct CommandIndex {
//     offset: u64
// }
//
// impl CommandIndex {
//     pub fn into_seek_from(self) -> SeekFrom{
//         SeekFrom::Start(self.offset)
//     }
// }

/// A persistent storage for kv;
#[derive(Debug)]
pub struct KvStore {
    fp: File,
    mem_map: HashMap<String, CommandIndex>,
}

impl KvStore {
    /// Initialize a new storage.
    pub fn open(pathinfo: impl Into<PathBuf>) -> Result<KvStore> {
        let mut pathbuf = pathinfo.into();
        pathbuf.push(FILE_PATH);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(pathbuf.as_path())
            .with_context(|| format!("Unable to open file at {:?}", pathbuf.as_path()))?;
        let mem_map =
            Self::replay(&mut file).with_context(|| format!("Failed when replay log."))?;
        Ok(Self { fp: file, mem_map })
    }
    fn replay(fp: &mut File) -> Result<HashMap<String, CommandIndex>> {
        fp.seek(SeekFrom::Start(0))?;
        let mut buf_reader = BufReader::new(fp);
        let mut map = HashMap::new();
        let mut line = String::new();
        loop {
            let pos = buf_reader
                .stream_position()
                .context(format!("Bad record. {}", line))?;
            match buf_reader.read_line(&mut line) {
                Ok(chara_num) => {
                    if chara_num == 0 {
                        return Ok(map);
                    } else {
                        let command = serde_json::from_str::<Command>(&line)
                            .with_context(|| format!("Error to parse JSON string: {}", &line))?;
                        match command {
                            Command::Insertion { key, .. } => {
                                map.insert(key, pos);
                            }
                            Command::Discard { key } => {
                                map.remove(&key);
                            }
                        }
                    }
                    line.clear();
                }
                Err(x) => {
                    return Err(anyhow::Error::from(x));
                }
            };
        }
    }
    /// Get the value by key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.mem_map = Self::replay(&mut self.fp)?;
        let idx = self.mem_map.get(&key);
        if idx.is_none() {
            return Ok(None);
        }
        let &idx = idx.unwrap();
        let mut buf_reader = BufReader::new(&mut self.fp);
        buf_reader.seek(SeekFrom::Start(idx))?;
        let mut json = String::new();
        buf_reader
            .read_line(&mut json)
            .with_context(|| "Error to get line.")?;
        let deserialized = serde_json::from_str::<Command>(json.trim())?;
        if let Command::Insertion {
            key: ikey,
            value: ivalue,
        } = deserialized
        {
            if ikey != key {
                bail!("Mismatched command key. {}", json);
            } else {
                Ok(Some(ivalue))
            }
        } else {
            bail!("Invalid command: {}", json)
        }
    }
    /// Set the value belonging to the provided key.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let idx = self.append_command(&Command::Insertion {
            key: key.clone(),
            value: value.clone(),
        })?;
        self.mem_map.insert(key, idx);
        Ok(())
    }

    fn append_command(&mut self, command: &Command) -> Result<CommandIndex> {
        let record_string = serde_json::to_string(&command)?;
        let idx = self
            .fp
            .stream_position()
            .context("Failed to get stream position of new record.");
        writeln!(self.fp, "{}", record_string).with_context(|| {
            format!(
                "Failed to write record into storage file. record: {}",
                record_string
            )
        })?;
        self.fp.flush()?;
        idx
    }
    /// Remove an key whether it exists or not.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.mem_map = Self::replay(&mut self.fp)?;
        match self.mem_map.remove(&key) {
            Some(_) => self
                .append_command(&Command::Discard { key: key.clone() })
                .map(|_| ()),
            None => Err(anyhow!("Key: {} not found.", key)),
        }
    }

    /// list all values.
    pub fn list(&mut self) -> Result<Vec<(String, String)>> {
        self.mem_map = Self::replay(&mut self.fp)?;
        let keys = self.mem_map.keys().map(String::clone).collect::<Vec<_>>();
        Ok(keys
            .into_iter()
            .map(|k| {
                (
                    k.clone(),
                    self.get(k.clone()).unwrap().unwrap_or("None".to_owned()),
                )
            })
            .collect())
    }
}
