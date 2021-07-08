use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::mem::swap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use serde::Deserialize;
use serde::Serialize;

use crate::{KvsEngine, Result};

const STORAGE_FILE_A: &str = "storage_A.db";
const STORAGE_FILE_B: &str = "storage_B.db";
const CONFIG_FILE_PATH: &str = "config.json";

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Insertion { key: String, value: String },
    Discard { key: String },
}

type CommandIndex = u64;

/// In memory key-value storage.
///
/// Key-value entry stores in `HashMap` temporarily.
///
///  Example:
/// ```rust
///# use kvs::KvStore;
///# use crate::kvs::KvsEngine;
///# extern crate tempfile;
///let temp_dir = tempfile::TempDir::new().unwrap();
///let mut store = KvStore::open(temp_dir.path()).unwrap();
///store.set("key1", "value1");
///assert_eq!(store.get("key1").unwrap(), Some("value1".to_string()));
/// ```
#[derive(Debug)]
pub struct KvStore {
    yolk: Arc<RwLock<KvStoreYolk>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KvStoreConfig {
    current_file: String,
    log_num: u64,
}

#[derive(Debug)]
struct KvStoreYolk {
    fp: File,
    config_file_path: PathBuf,
    mem_map: HashMap<String, CommandIndex>,
    data_dir: PathBuf,
    log_num: u64,
    current_file: String,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            yolk: Arc::clone(&self.yolk),
        }
    }
}

impl KvStore {
    fn get_path(dir: &Path, file_name: &str) -> PathBuf {
        let mut dir = dir.to_path_buf();
        dir.push(file_name);
        dir
    }
    /// Initialize a new storage.
    pub fn open(pathinfo: impl Into<PathBuf>) -> Result<KvStore> {
        let path_buf = pathinfo.into();
        let config_file_path = Self::get_path(&path_buf, CONFIG_FILE_PATH);
        let (current_file_name, num_logs) = {
            let config: KvStoreConfig = File::open(&config_file_path)
                .context("Failed to open json configuration file.")
                .and_then(|fp| serde_json::from_reader(&fp).context("Invalid file content"))
                .unwrap_or(KvStoreConfig {
                    current_file: STORAGE_FILE_A.to_string(),
                    log_num: 0,
                });
            (config.current_file, config.log_num)
        };
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(Self::get_path(&path_buf, &current_file_name))
            .with_context(|| format!("Unable to open file at {:?}", path_buf.as_path()))?;
        let mem_map = Self::replay(&mut file).context("Failed when replay log.")?;
        Ok(Self {
            yolk: Arc::new(RwLock::new(KvStoreYolk {
                data_dir: path_buf,
                fp: file,
                config_file_path,
                mem_map,
                log_num: num_logs,
                current_file: current_file_name,
            })),
        })
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

    fn query_command_index(&idx: &CommandIndex, fp: &File) -> Result<(Command, String)> {
        let mut buf_reader = BufReader::new(fp);
        buf_reader.seek(SeekFrom::Start(idx))?;
        let mut json = String::new();
        buf_reader
            .read_line(&mut json)
            .with_context(|| "Error to get line.")?;
        Ok(((serde_json::from_str::<Command>(json.trim())?), json))
    }

    fn append_command(fp: &mut File, command: &Command) -> Result<CommandIndex> {
        fp.seek(SeekFrom::End(0))?;
        let record_string = serde_json::to_string(&command)?;
        let idx = fp
            .stream_position()
            .context("Failed to get stream position of new record.");
        writeln!(fp, "{}", record_string).with_context(|| {
            format!(
                "Failed to write record into storage file. record: {}",
                record_string
            )
        })?;
        idx
    }

    /// list all values.
    // pub fn list(&mut self) -> Result<Vec<(String, String)>> {
    pub fn list(&mut self) -> Result<()> {
        bail!("Not implemented yet.")
    }

    /// compaction.
    pub fn compaction(&self) -> Result<()> {
        let mut yolk = self.yolk.write().unwrap();
        let mut pathbuf = yolk.data_dir.clone();
        let spare_file = if yolk.current_file == STORAGE_FILE_A {
            STORAGE_FILE_B
        } else {
            STORAGE_FILE_A
        };
        pathbuf.push(spare_file);
        let mut temp_fp = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&pathbuf)
            .with_context(|| {
                format!(
                    "Failed when creating temporary file for compaction. {:?}",
                    pathbuf.as_path()
                )
            })?;
        let mem_map = Self::replay(&mut yolk.fp)?;
        let mut num = 0u64;
        for (key, idx) in mem_map.into_iter() {
            let val = {
                let (deserialized, json) = Self::query_command_index(&idx, &mut yolk.fp)?;
                if let Command::Insertion {
                    key: ikey,
                    value: ivalue,
                } = deserialized
                {
                    if ikey != key {
                        bail!("Mismatched command key. {}", json);
                    } else {
                        ivalue
                    }
                } else {
                    bail!("Invalid command: {}", json)
                }
            };
            let idx = temp_fp.stream_position()?;
            let cmd_string = serde_json::to_string(&Command::Insertion {
                key: key.clone(),
                value: val,
            })?;
            writeln!(temp_fp, "{}", cmd_string)?;
            yolk.mem_map.insert(key, idx);
            num += 1;
        }
        swap(&mut yolk.fp, &mut temp_fp);
        temp_fp.set_len(0)?;
        yolk.log_num = num;
        yolk.current_file = spare_file.to_string();
        Ok(())
    }

    fn sync_config(&self) -> Result<()> {
        let yolk = self.yolk.write().unwrap();
        let mut config_fp = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&yolk.config_file_path)?;
        let serialized = serde_json::to_string(&KvStoreConfig {
            current_file: yolk.current_file.clone(),
            log_num: yolk.log_num,
        })?;
        config_fp
            .write_all(serialized.as_ref())
            .context("Error to sync config to config file.")
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        self.flush().unwrap();
        self.sync_config().unwrap()
    }
}

impl KvsEngine for KvStore {
    /// Get the value by key.
    fn get(&self, key: &str) -> Result<Option<String>> {
        //WARNING 为了实现并发读，需要通过File::Open打开多个不同的File对象，而不是try_clone
        //回头再改.2021年7月7日
        let mut yolk = self.yolk.write().unwrap();
        let idx = yolk.mem_map.get(key).cloned();
        if idx.is_none() {
            return Ok(None);
        }
        let (deserialized, json) = Self::query_command_index(&idx.unwrap(), &yolk.fp)?;
        if let Command::Insertion { key: ikey, value } = deserialized {
            if ikey != key {
                bail!("Mismatched command key. {}", json);
            } else {
                Ok(Some(value))
            }
        } else {
            bail!("Invalid command: {}", json)
        }
    }

    /// Set the value belonging to the provided key.
    fn set(&self, key: &str, value: &str) -> Result<()> {
        let mut yolk = self.yolk.write().unwrap();
        let idx = Self::append_command(
            &mut yolk.fp,
            &Command::Insertion {
                key: key.to_string(),
                value: value.to_string(),
            },
        )?;
        yolk.log_num += 1;
        yolk.mem_map.insert(key.to_string(), idx);
        if yolk.log_num > (2 * yolk.mem_map.len()) as u64 {
            drop(yolk);
            self.compaction()?;
        }
        Ok(())
    }

    /// Remove an key whether it exists or not.
    fn remove(&self, key: &str) -> Result<()> {
        let mut yolk = self.yolk.write().unwrap();
        yolk.mem_map = Self::replay(&mut yolk.fp)?;
        match yolk.mem_map.remove(key) {
            Some(_) => {
                let result = Self::append_command(
                    &mut yolk.fp,
                    &Command::Discard {
                        key: key.to_string(),
                    },
                )
                .map(|_| ());
                yolk.log_num += 1;
                result
            }
            None => Err(anyhow!("Key: {} not found.", key)),
        }
    }

    fn flush(&self) -> Result<()> {
        self.yolk.write().map_or_else(
            |_| Err(anyhow!("Mutex poisoned.")),
            |mut o| Ok(o.fp.flush()?),
        )
    }
}
