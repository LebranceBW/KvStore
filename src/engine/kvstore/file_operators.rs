use super::Result;
use crate::engine::kvstore::kvstore::CommandPos;
use crate::engine::kvstore::Command;
use std::fs::File;
use std::sync::{Arc, Mutex};

pub type FileID = usize;

pub type FileOffset = u64;

#[derive(Debug, Clone)]
pub struct FileReader {}
impl FileReader {
    pub fn query_command(&self, pos: FileOffset) -> Result<Command> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FileWriter {
    file: Arc<Mutex<File>>,
}
impl FileWriter {
    pub fn insert_command(&self, command: Command) -> Result<CommandPos> {
        todo!()
    }
}
