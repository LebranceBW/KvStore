use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::io::Write;
use std::path::PathBuf;

use anyhow::Context;

use crate::engine::kvstore::Command;
use crate::engine::kvstore::kvstore::CommandPos;

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

    pub fn take_over_file(file: File, id: FileID, dir: &PathBuf) -> Self {
        Self {
            reader: BufReader::new(file),
            file_id: id,
            file_path: file_path_from_id(id, dir),
        }
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

    pub fn command_iter(&self) -> impl Iterator<Item=(Command, CommandPos)> {
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
    type Item = (Command, CommandPos);

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.reader.stream_position()
            .ok();
        pos.and_then(|pos|
            {
                let mut buf = String::new();
                self.reader.read_line(&mut buf)
                    .context("")
                    .and_then(|_| {
                        serde_json::from_str::<Command>(&buf)
                            .with_context(|| format!("Failed to parse json"))
                    })
                    .ok()
                    .map(|cmd| (cmd, CommandPos {
                        file_id: self.id,
                        pos,
                    }))
            }
        )
    }
}

#[derive(Debug)]
pub(crate) struct FileWriter {
    pub(crate) file: File,
    pub(crate) file_id: FileID,
}

impl FileWriter {
    pub fn open(dir: impl Into<PathBuf>, id: FileID) -> Result<Self> {
        let dir_path = dir.into();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path_from_id(id, &dir_path))
            .expect(&format!("Failed to open file {:?}", file_path_from_id(id,
                                                                           &dir_path)));
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            file_id: id,
        })
    }
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()
            .with_context(|| format!("Failed to flush the cache on disk. file_id: {}", self.file_id))
    }
    pub fn insert_command(&mut self, command: &Command) -> Result<CommandPos> {
        let record_string = serde_json::to_string(command)
            .with_context(|| format!("Failed to serialize Command. {:?}", command))?;
        let stream_pos = self
            .file
            .stream_position()
            .context("Failed to get stream position of new record.")?;
        writeln!(self.file, "{}", record_string)
            .with_context(|| format!("Failed to write file."))
            .map(|_| self.file.flush()
                .context("Failed when sync to disk."))
            .and(Ok(CommandPos {
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

pub(crate) fn open_new_file(dir: &PathBuf, file_id: FileID) -> Result<(File, PathBuf)> {
    let path = file_path_from_id(file_id, dir);
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path.as_path())
        .with_context(|| format!("Failed to open a new file: {:?}", path))
        .map(|fp| (fp, path))
}
