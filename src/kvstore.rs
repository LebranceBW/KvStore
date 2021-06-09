use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufReader, SeekFrom};
use std::io::prelude::*;
use std::mem::swap;
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use serde::Deserialize;
use serde::Serialize;

use crate::Result;

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
///let mut store = KvStore::new();
///store.set("key1", "value1");
///assert_eq!(store.get("key1"), Some("value1"));
/// ```
#[derive(Debug)]
pub struct KvStore {
    fp: File,
    config_file_path: PathBuf,
    mem_map: HashMap<String, CommandIndex>,
    data_dir: PathBuf,
    log_num: u64,
    current_file: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KvStoreConfig {
    current_file: String,
    log_num: u64,
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
            let config: KvStoreConfig =
                File::open(&config_file_path)
                    .context("Failed to open json configuration file.")
                    .and_then(
                        |fp|
                            serde_json::from_reader(&fp)
                                .context("Invalid file content")
                    ).unwrap_or(
                    KvStoreConfig {
                        current_file: STORAGE_FILE_A.to_string(),
                        log_num: 0,
                    }
                );
            (config.current_file, config.log_num)
        };
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(
                Self::get_path(&path_buf, &current_file_name)
            )
            .with_context(|| format!("Unable to open file at {:?}", path_buf.as_path()))?;
        let mem_map = Self::replay(&mut file).
            context("Failed when replay log.")?;
        Ok(Self {
            data_dir: path_buf,
            fp: file,
            config_file_path,
            mem_map,
            log_num: num_logs,
            current_file: current_file_name,
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

    fn query_command_index(&idx: &CommandIndex, fp: &mut File) -> Result<(Command, String)> {
        let mut buf_reader = BufReader::new(fp);
        buf_reader.seek(SeekFrom::Start(idx))?;
        let mut json = String::new();
        buf_reader
            .read_line(&mut json)
            .with_context(|| "Error to get line.")?;
        Ok(((serde_json::from_str::<Command>(json.trim())?), json))
    }
    /// Get the value by key.
    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let idx = self.mem_map.get(key);
        if idx.is_none() {
            return Ok(None);
        }
        let (deserialized, json) =
            Self::query_command_index(idx.unwrap(), &mut self.fp)?;
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
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let idx = self.append_command(&Command::Insertion {
            key: key.to_string(),
            value: value.to_string(),
        })?;
        self.mem_map.insert(key.to_string(), idx);
        if self.log_num > (2 * self.mem_map.len()) as u64 {
            self.compaction()?;
        }
        Ok(())
    }

    fn append_command(&mut self, command: &Command) -> Result<CommandIndex> {
        self.fp.seek(SeekFrom::End(0))?;
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
        self.log_num += 1;
        idx
    }
    /// Remove an key whether it exists or not.
    pub fn remove(&mut self, key: &str) -> Result<()> {
        self.mem_map = Self::replay(&mut self.fp)?;
        match self.mem_map.remove(key) {
            Some(_) => self
                .append_command(&Command::Discard { key: key.to_string() })
                .map(|_| ()),
            None => Err(anyhow!("Key: {} not found.", key)),
        }
    }

    /// list all values.
    // pub fn list(&mut self) -> Result<Vec<(String, String)>> {
    pub fn list(&mut self) -> Result<()> {
        bail!("Not implemented yet.")
    }

    /// compaction.
    pub fn compaction(&mut self) -> Result<()> {
        let mut pathbuf = self.data_dir.clone();
        let spare_file = if self.current_file == STORAGE_FILE_A {
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
            .with_context(|| format!("Failed when creating temporary file for compaction. {:?}", pathbuf.as_path()))?;
        let mem_map = Self::replay(&mut self.fp)?;
        let mut num = 0u64;
        for (key, _) in mem_map.into_iter() {
            let val = self.get(&key)?.unwrap();
            let idx = temp_fp.stream_position()?;
            let cmd_string = serde_json::to_string(&Command::Insertion {
                key: key.clone(),
                value: val,
            })?;
            writeln!(temp_fp, "{}", cmd_string)?;
            self.mem_map.insert(key, idx);
            num += 1;
        }
        swap(&mut self.fp, &mut temp_fp);
        temp_fp.set_len(0)?;
        self.log_num = num;
        self.current_file = spare_file.to_string();
        Ok(())
    }

    fn sync_config(&mut self) -> Result<()> {
        let mut config_fp = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.config_file_path)?;
        let serialized = serde_json::to_string(
            &KvStoreConfig {
                current_file: self.current_file.clone(),
                log_num: self.log_num,
            },
        )?;
        config_fp.write_all(serialized.as_ref())
            .context("Error to sync config to config file.")
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        self.sync_config().unwrap()
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use assert_cmd::prelude::*;
    use predicates::prelude::*;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn config_test() -> Result<()> {
        // let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStore::open("./")?;
        // let mut store = KvStore::open(temp_dir.path())?;
        store.set("key1", "value1");
        store.set("key1", "value1");
        store.set("key1", "value1");
        store.set("key1", "value1");
        store.set("key1", "value1");
        store.set("key1", "value1");
        Ok(assert_eq!(store.log_num, 6))
    }

    #[test]
    fn config_test2() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        for _ in 0..3 {
            std::process::Command::cargo_bin("kvs")
                .unwrap()
                .args(&["set", "key1", "value1"])
                .current_dir(&temp_dir)
                .assert()
                .success();
        }
    }
}