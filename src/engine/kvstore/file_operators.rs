use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;

use anyhow::Context;

use crate::engine::kvstore::kvstore::CommandPosition;
use crate::engine::kvstore::Command;

use super::Result;

pub type FileID = usize;

pub type FileOffset = u64;

/// Buggy 点，每次读取同一个文件都需要重新打开，需要优化
#[derive(Debug)]
pub struct FileReader {
    reader: BufReader<File>,
    file_id: FileID,
    file_path: PathBuf,
}

impl Clone for FileReader {
    fn clone(&self) -> Self {
        let reader = OpenOptions::new()
            .read(true)
            .open(&self.file_path)
            .map(|fp| BufReader::new(fp))
            .expect(&format!("Failed to open file {:?}", self.file_path));
        Self {
            reader,
            file_id: self.file_id,
            file_path: self.file_path.clone(),
        }
    }
}

impl FileReader {
    pub fn open(dir: impl Into<PathBuf>, id: FileID) -> Result<Self> {
        let path_buf = file_path_from_id(id, dir);
        let reader = OpenOptions::new()
            .read(true)
            .open(&path_buf)
            .map(|fp| BufReader::new(fp))?;
        Ok(Self {
            reader,
            file_id: id,
            file_path: path_buf,
        })
    }

    pub fn readline_at(&mut self, pos: FileOffset) -> Result<String> {
        self.reader.seek(SeekFrom::Start(pos))?;
        let mut ret = String::new();
        self.reader
            .read_line(&mut ret)
            .with_context(|| "Error to get line.")
            .and(Ok(ret))
    }
    pub fn query_command(&self, pos: FileOffset) -> Result<Command> {
        let mut buf_reader = FileReader::clone(self).reader;
        buf_reader.seek(SeekFrom::Start(pos))?;
        let mut json = String::new();
        buf_reader
            .read_line(&mut json)
            .with_context(|| "Error to get line.")?;
        Ok(serde_json::from_str::<Command>(json.trim())?)
    }

    pub fn command_iter(&self) -> impl Iterator<Item = (Command, CommandPosition)> {
        let mut buf_reader = FileReader::clone(self).reader;
        buf_reader.seek(SeekFrom::Start(0)).unwrap();
        CommandIter {
            reader: buf_reader,
            id: self.file_id,
        }
    }
    pub fn remove_file(self) -> Result<()> {
        std::fs::remove_file(&self.file_path)
            .with_context(|| format!("Failed to remove outdated file: {:?}", self.file_path))
    }
}

pub struct CommandIter {
    reader: BufReader<File>,
    id: FileID,
}

impl Iterator for CommandIter {
    type Item = (Command, CommandPosition);

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.reader.stream_position().ok();
        pos.and_then(|pos| {
            let mut buf = String::new();
            self.reader
                .read_line(&mut buf)
                .context("")
                .and_then(|_| {
                    serde_json::from_str::<Command>(&buf)
                        .with_context(|| format!("Failed to parse json"))
                })
                .ok()
                .map(|cmd| {
                    (
                        cmd,
                        CommandPosition {
                            file_id: self.id,
                            pos,
                        },
                    )
                })
        })
    }
}

#[derive(Debug)]
pub(crate) struct FileWriter {
    pub(crate) file: File,
    pub(crate) file_id: FileID,
    pub total_size: usize,
}

impl FileWriter {
    pub fn open(dir: impl Into<PathBuf>, id: FileID) -> Result<Self> {
        let dir_path = dir.into();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path_from_id(id, &dir_path))
            .expect(&format!(
                "Failed to open file {:?}",
                file_path_from_id(id, &dir_path)
            ));
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            file_id: id,
            total_size: 0,
        })
    }
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush().with_context(|| {
            format!(
                "Failed to flush the cache on disk. file_id: {}",
                self.file_id
            )
        })
    }

    pub fn append_serialized_command(&mut self, str: &str) -> Result<CommandPosition> {
        let pos = self.file.stream_position()?;
        let size = self
            .file
            .write(str.as_bytes())
            .context("Failed to write str")?;
        self.total_size += size;
        Ok(CommandPosition {
            file_id: self.file_id,
            pos,
        })
    }

    pub fn get_total_size(&self) -> usize {
        self.total_size
    }

    pub fn append_command(&mut self, command: &Command) -> Result<CommandPosition> {
        let mut record_string = serde_json::to_string(command)
            .with_context(|| format!("Failed to serialize Command. {:?}", command))?;
        record_string.push('\n');
        let stream_pos = self
            .file
            .stream_position()
            .context("Failed to get stream position of new record.")?;
        self.file
            .write(record_string.as_bytes())
            .with_context(|| format!("Failed to write file."))
            .map(|cnt| {
                self.total_size += cnt;
            })
            .and(Ok(CommandPosition {
                file_id: self.file_id,
                pos: stream_pos,
            }))
    }
}

fn file_name_from_id(file_id: FileID) -> String {
    format!("{:05}.log", file_id)
}

fn file_path_from_id(file_id: FileID, dir: impl Into<PathBuf>) -> PathBuf {
    dir.into().join(&file_name_from_id(file_id))
}
