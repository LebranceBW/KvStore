use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Context};
use anyhow::bail;
use log::*;
use serde::{Deserialize, Serialize};

use config::*;

use crate::engine::kvstore::file_operators::FileOffset;
use crate::KvsEngine;

use super::Command;
use super::file_operators::FileID;
use super::file_operators::FileReader;
use super::file_operators::FileWriter;
use super::Result;

// Use to locate the command
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandPosition {
    pub(crate) file_id: FileID,
    pub(crate) pos: FileOffset,
}

/// KvStorage implement by my self.
/// Example usage:
/// ```rust
/// # extern crate tempfile;
/// # extern crate anyhow;
/// # extern crate kvs;
/// # use kvs::engine::{KvsEngine, KvStore};
/// # use tempfile::TempDir;
/// # use anyhow::Result;
/// # fn main() -> Result<()>{
///   let temp_dir = TempDir::new().expect("unable to create temporary working directory");
///   let mut store = KvStore::open(temp_dir.path())?;
///
///   store.set("key1", "value1")?;
///   store.set("key2", "value2")?;
///
///   assert_eq!(store.get("key1")?, Some("value1").map(str::to_string));
///   assert_eq!(store.get("key2")?, Some("value2").map(str::to_string));
///
///   store.remove("key1")?;
///   assert_eq!(store.get("key1").unwrap(), None);
///   Ok(())
/// # }
/// ```
pub struct KvStore {
    inner: Arc<RwLock<KvStoreInner>>,
}

impl KvStore {
    /// Open a new instance in `dir`
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let inner = KvStoreInner::open(dir)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(inner))
        })
    }
}

struct KvStoreInner {
    idx_map: HashMap<String, CommandPosition>,
    readers: HashMap<FileID, FileReader>,
    writer: FileWriter,
    uncompacted_num: usize,
    id_generator: CycleCounter,
    current_dir: PathBuf,
    compaction_threshold: usize,
}

impl KvStoreInner {
    pub fn retrieving_from_disk(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir_path = dir.into();
        let dump_file = dir_path.join(DUMP_FILE_NAME);
        // recover from existing file
        let PersistentStruct {
            compaction_threshold,
            frozen_idx_map: mut idx_map,
            uncompacted_size: mut uncompacted,
        } = PersistentStruct::restore_from_file(dump_file.as_path())?;
        let existing_file_id = Self::log_file_lists(&dir_path);
        let readers = existing_file_id
            .iter()
            .map(|&file_id| {
                (file_id, FileReader::open(&dir_path, file_id)
                    .expect(&format!("Failed to open file for reading, id: {}", file_id)))
            })
            .collect::<HashMap<_, _>>();
        let unmerged_file_id = existing_file_id.into_iter().max().unwrap();
        idx_map = Self::replay(idx_map, &readers[&unmerged_file_id], &mut uncompacted);
        let writer = FileWriter::open(
            &dir_path,
            unmerged_file_id,
        )?;
        // (idx_map, readers, unmerged_file_id)
        Ok(Self {
            idx_map,
            readers,
            writer,
            uncompacted_num: uncompacted,
            current_dir: dir_path,
            id_generator: CycleCounter::new(unmerged_file_id,
                                            MAX_FILE_ID),
            compaction_threshold,
        })
    }
    pub fn create_new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir_path = dir.into();
        let mut readers = HashMap::new();
        let writer = FileWriter::open(
            &dir_path,
            0,
        )?;
        readers.insert(
            0,
            FileReader::open(&dir_path, 0)
                .expect(&format!("Failed to open file for reading: {}", 0)),
        );
        let dump_file = dir_path.join(DUMP_FILE_NAME);
        PersistentStruct::dump_to_file(PersistentStruct {
            frozen_idx_map: Default::default(),
            uncompacted_size: 0,
            compaction_threshold: 64,
        }, &dump_file)?;
        Ok(Self {
            idx_map: Default::default(),
            readers,
            writer,
            id_generator: CycleCounter::new(1, MAX_FILE_ID),
            current_dir: dir_path,
            uncompacted_num: 0,
            compaction_threshold: 64,
        })
    }
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let dump_file = dir.join(DUMP_FILE_NAME);
        if dump_file.exists() {
            Self::retrieving_from_disk(dir)
        } else {
            Self::create_new(dir)
        }
    }

    #[allow(unused)]
    pub fn uncompacted_record_num(&self) -> usize {
        self.uncompacted_num
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let record = self.idx_map.get(key);
        if record.is_none() {
            return Ok(None);
        }
        let cmd_pos = record.unwrap();
        let command = self
            .readers
            .get(&cmd_pos.file_id)
            .ok_or(anyhow!("Failed to find file, id:{}", cmd_pos.file_id))
            .and_then(|entry| entry.query_command(cmd_pos.pos))?;
        if let Command::Insertion { key: ikey, value } = command {
            if ikey == key {
                return Ok(Some(value));
            } else {
                bail!("Key mismatched. Actual: {}, Expected: {}", ikey, key)
            }
        } else {
            bail!("Mismatched command: {:?}", command)
        }
    }

    fn log_file_lists(dir: &Path) -> Vec<FileID> {
        let mut lst: Vec<_> = std::fs::read_dir(&dir).unwrap()
            .flat_map(|res| -> Result<_> { Ok(res?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|s| s.trim_end_matches(".log"))
                    .map(str::parse::<usize>)
            })
            .flatten()
            .collect();
        lst.sort_unstable();
        lst
    }

    fn compaction(&mut self) -> Result<()> {
        info!("Uncompacted records reaches {}, compaction triggered.", self.uncompacted_num);
        let (mut new_idx_map, mut new_reader_map) = (HashMap::new(), HashMap::new());
        let (mut file_id, _size_cnt) = (self.id_generator.next().unwrap(), 0usize);
        let mut writer = FileWriter::open(&self.current_dir, file_id)?;
        for (key, cmd_pos) in self.idx_map.drain() {
            let command_str = self
                .readers
                .get_mut(&cmd_pos.file_id)
                .ok_or(anyhow!("Failed to find file, id:{}.", cmd_pos.file_id))
                .and_then(|entry| entry.readline_at(cmd_pos.pos))?;
            let pos = writer.append_serialized_command(
                &command_str
            )?;
            new_idx_map.insert(key, pos);
            if writer.get_total_size() > MAX_FILE_SIZE {
                new_reader_map.insert(
                    file_id,
                    FileReader::open(
                        &self.current_dir,
                        file_id,
                    )?,
                );
                file_id = self.id_generator.next().unwrap();
                writer = FileWriter::open(
                    &self.current_dir,
                    file_id,
                )?;
            }
        }
        new_reader_map.insert(file_id,
                              FileReader::open(
                                  &self.current_dir,
                                  file_id,
                              )?);
        self.writer = writer;
        self.uncompacted_num = 0;
        self.compaction_threshold *= 2;
        std::mem::swap(&mut new_idx_map, &mut self.idx_map);
        std::mem::swap(&mut new_reader_map, &mut self.readers);
        let dump_file = self.current_dir.join(DUMP_FILE_NAME);
        PersistentStruct {
            compaction_threshold: self.compaction_threshold,
            frozen_idx_map: self.idx_map.clone(),
            uncompacted_size: self.uncompacted_num,
        }.dump_to_file(
            &dump_file
        )?;
        // remove compacted files
        for (_, file) in new_reader_map.into_iter() {
            file.remove_file()?;
        }
        self.writer.flush()?;
        //generate hint file
        Ok(())
    }

    fn replay(mut idx_map: HashMap<String, CommandPosition>, reader: &FileReader, uncompacted_items: &mut usize)
              -> HashMap<String, CommandPosition> {
        for (command, command_pos) in reader.command_iter() {
            trace!("Replaying: Command:{:?} at {:?}", command, command_pos);
            match command {
                Command::Insertion { key, .. } => {
                    if idx_map.insert(key, command_pos).is_some() {
                        *uncompacted_items += 1;
                    }
                }
                Command::Discard { key } => {
                    idx_map.remove(&key);
                    *uncompacted_items += 2;
                }
            }
        }
        idx_map
    }

    #[inline]
    fn need_compaction(&self) -> bool {
        self.uncompacted_num > self.compaction_threshold
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let command = Command::Insertion {
            key: key.to_string(),
            value: value.to_string(),
        };
        {
            let writer = &mut self.writer;
            writer
                .append_command(&command)
                .map(|pos| self.idx_map.insert(key.to_string(), pos))
                .map(|op| {
                    if op.is_some() {
                        self.uncompacted_num += 1;
                    }
                })?;
        };
        let total_size = self.writer.get_total_size();
        if total_size > MAX_FILE_SIZE {
            let next_id =
                self.id_generator.next().unwrap();
            self.writer = FileWriter::open(
                &self.current_dir,
                next_id,
            )?;
            self.readers.insert(
                next_id,
                FileReader::open(&self.current_dir,
                                 next_id)?,
            );
        }
        if self.need_compaction() {
            self.compaction()?;
        }
        Ok(())
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        let exists = self.idx_map.contains_key(key);
        if exists {
            let command = Command::Discard {
                key: key.to_string(),
            };
            let writer = &mut self.writer;
            match writer.append_command(&command)
            {
                Ok(_) => {
                    self.idx_map.remove(key);
                    Ok(())
                }
                Err(_) => {
                    bail!("Failed to make record onto disk.")
                }
            }
        } else {
            bail!("Key: {} not found.", key)
        }
    }
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl KvsEngine for KvStore {
    fn get(&self, key: &str) -> Result<Option<String>> {
        self.inner.read()
            .map_err(|_| anyhow!("Failed to acquire read lock."))
            .and_then(|inner|
                KvStoreInner::get(&inner, key)
            )
    }

    fn set(&self, key: &str, value: &str) -> Result<()> {
        self.inner.write()
            .map_err(|_| anyhow!("Failed to acquire write lock."))
            .and_then(
                |mut inner|
                    inner.set(key, value)
            )
    }

    fn remove(&self, key: &str) -> Result<()> {
        self.inner.write()
            .map_err(|_| anyhow!("Failed to acquire write lock."))
            .and_then(
                |mut inner|
                    inner.remove(key)
            )
    }
}

struct CycleCounter {
    count: usize,
    maximum: usize,
}

impl CycleCounter {
    pub fn new(count: usize, maxinum: usize) -> Self {
        Self {
            count,
            maximum: maxinum,
        }
    }
}

impl Iterator for CycleCounter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let cnt = self.count;
        self.count =  (self.count + 1) % self.maximum;
        Some(cnt)
    }
}

mod config {
    pub const DUMP_FILE_NAME: &'static str = ".dumpfile";
    pub const MAX_FILE_ID: usize = 1 << 16;
    pub const MAX_FILE_SIZE: usize = 100 * 1 << 20;
}

/// 辅助保存KvStore当前状态的结构体
#[derive(Deserialize, Serialize)]
struct PersistentStruct {
    pub compaction_threshold: usize,
    pub frozen_idx_map: HashMap<String, CommandPosition>,
    pub uncompacted_size: usize,
}

impl PersistentStruct {
    pub fn dump_to_file(self, file_path: &Path) -> Result<()> {
        let fp = OpenOptions::new()
            .truncate(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        serde_json::to_writer(fp, &self)
            .with_context(|| format!("failed to dump onto {:?}.", file_path))
    }

    pub fn restore_from_file(file_path: &Path) -> Result<Self> {
        let fp = OpenOptions::new()
            .read(true)
            .create(false)
            .open(file_path)?;
        serde_json::from_reader(fp)
            .with_context(|| format!("failed to restore from {:?}.", file_path))
    }
}

#[cfg(test)]
mod test {
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use tempfile::TempDir;
    use walkdir::WalkDir;

    use super::*;

    #[test]
    fn basic_usage() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStoreInner::open(temp_dir.path())?;

        store.set("key1", "value1")?;
        store.set("key2", "value2")?;

        assert_eq!(store.get("key1")?, Some("value1").map(str::to_string));
        assert_eq!(store.get("key2")?, Some("value2").map(str::to_string));

        // Open from disk again and check persistent data.
        drop(store);
        let store = KvStoreInner::open(temp_dir.path())?;
        assert_eq!(store.get("key1")?, Some("value1").map(str::to_string));
        assert_eq!(store.get("key2")?, Some("value2").map(str::to_string));

        Ok(())
    }

    #[test]
    fn overwrite_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStoreInner::open(temp_dir.path())?;

        store.set("key1", "value1")?;
        assert_eq!(store.get("key1")?, Some("value1").map(str::to_string));
        store.set("key1", "value2")?;
        assert_eq!(store.get("key1")?, Some("value2").map(str::to_string));

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = KvStoreInner::open(temp_dir.path())?;
        assert_eq!(store.get("key1")?, Some("value2").map(str::to_string));
        store.set("key1", "value3")?;
        let val = store.get("key1")?;
        let expected = Some("value3").map(str::to_string);
        assert_eq!(expected, val,
                   "Value stored by KvStore: {:?}, expected: {:?}", val, expected);

        Ok(())
    }

    #[test]
    fn get_non_existent_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStoreInner::open(temp_dir.path())?;

        store.set("key1", "value1")?;
        assert_eq!(store.get("key2")?, None);

        // Open from disk again and check persistent data.
        drop(store);
        let store = KvStoreInner::open(temp_dir.path())?;
        assert_eq!(store.get("key2")?, None);

        Ok(())
    }

    #[test]
    fn remove_non_existent_key() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStoreInner::open(temp_dir.path())?;
        assert!(store.remove("key1").is_err());
        Ok(())
    }

    #[test]
    fn remove_key() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStoreInner::open(temp_dir.path())?;
        store.set("key1", "value1")?;
        assert!(store.remove("key1").is_ok());
        assert_eq!(store.get("key1")?, None);
        Ok(())
    }

    // Insert data until total size of the directory decreases.
// Test data correctness after compaction.
    #[test]
    fn compaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStoreInner::open(temp_dir.path())?;

        let dir_size = || {
            let entries = WalkDir::new(temp_dir.path()).into_iter();
            let len: walkdir::Result<u64> = entries
                .map(|res| {
                    res.and_then(|entry| entry.metadata())
                        .map(|metadata| metadata.len())
                })
                .sum();
            len.expect("fail to get directory size")
        };

        let mut current_size = dir_size();
        for iter in 0..1000 {
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                let value = format!("{}", iter);
                store.set(&key, &value)?;
            }

            let new_size = dir_size();
            if new_size > current_size {
                current_size = new_size;
                continue;
            }
            // Compaction triggered.

            drop(store);
            // reopen and check content.
            let store = KvStoreInner::open(temp_dir.path())?;
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                assert_eq!(store.get(&key)?, Some(format!("{}", iter)));
            }
            return Ok(());
        }

        panic!("No compaction detected");
    }

    #[test]
    pub fn huge_test() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStoreInner::open(temp_dir.path()).unwrap();
        for i in 0..9000 {
            store.set(&format!("key{}", i), &format!("key{}", i)).unwrap();
        }
        drop(store);
        let store = KvStoreInner::open(temp_dir.path()).unwrap();

        for i in (0..9000).rev() {
            assert_eq!(store.get(&format!("key{}", i)).unwrap(), Some(format!("key{}", i)))
        }
    }

    #[test]
    pub fn stress_test() {
        let test_set: Vec<(String, String)> = {
            let mut rng = rand::thread_rng();
            (1..30)
                .map(move |_| {
                    let key = random_string(rng.gen_range(1..100000));
                    let value = random_string(rng.gen_range(1..100000));
                    (key, value)
                })
                .collect::<Vec<_>>()
        };
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        println!("{:?}", temp_dir.path());
        let mut store = KvStoreInner::open(temp_dir.path()).unwrap();
        let len = 100;
        for _ in 0..len {
            for (key, value) in test_set.iter() {
                store.set(key, value).unwrap();
            }
        }
        for (key, value) in test_set.iter() {
            assert_eq!(store.get(key).unwrap().unwrap(), *value);
        }
    }

    fn random_string(len: usize) -> String {
        let rng = rand::thread_rng();
        rng.sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }
}