use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::bail;
use lockfree::map::Map;

use crate::engine::kvstore::file_operators::FileOffset;
use crate::engine::KvsEngine;

use super::file_operators::FileID;
use super::file_operators::FileReader;
use super::file_operators::FileWriter;
use super::Command;
use super::Result;

// Use to locate the command
#[derive(Clone)]
pub struct CommandPos {
    file_id: FileID,
    pos: FileOffset,
}

pub struct KvStore {
    idx_map: Map<String, CommandPos>,
    reader: Map<FileID, FileReader>,
    writer: FileWriter,
}

impl KvStore {
    pub fn open(dir: impl Into<PathBuf>) -> Self {
        todo!()
    }

    fn compaction(&mut self) -> Result<()> {
        todo!()
    }
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl KvsEngine for KvStore {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let record = self.idx_map.get(key);
        if record.is_none() {
            return Ok(None);
        }
        // let cmd_pos = record.unwrap().val().clone();
        let cmd_pos = record.unwrap()
            .val()
            .clone();
        let command = self
            .reader
            .get(&cmd_pos.file_id)
            .ok_or(anyhow!("Failed to find file."))
            .and_then(|entry| entry.val().query_command(cmd_pos.pos))?;
        if let Command::Insertion { key: ikey, value } = command {
            if ikey == key {
                return Ok(Some(value));
            }
            else {
                bail!("Key mismatched. Actual: {}, Expected: {}", ikey, key)
            }
        }
        else {
            bail!("Mismatched command: {:?}", command)
        }
    }
    fn set(&self, key: &str, value: &str) -> Result<()> {
        let command = Command::Insertion {
            key: key.to_string(),
            value: value.to_string(),
        };
        self.writer.insert_command(command).map(|pos| {
            self.idx_map.insert(key.to_string(), pos);
        })
    }
    fn remove(&self, key: &str) -> Result<()> {
        let command = Command::Discard {
            key: key.to_string(),
        };
        self.writer.insert_command(command).and_then(|pos| {
            self.idx_map
                .remove(key)
                .ok_or(anyhow!("No such record"))
                .map(|_| {})
        })
    }
}
