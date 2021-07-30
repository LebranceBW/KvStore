use std::path::PathBuf;

use anyhow::bail;
use anyhow::Context;
use sled::{Db, IVec};

use crate::KvsEngine;

use anyhow::Result;
#[derive(Clone)]
/// Adapter for sled engine.
pub struct SledAdapter {
    db: Db,
}

impl SledAdapter {
    /// create
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self {
            db: sled::open(path.into())?,
        })
    }

    fn ivec_from_str(s: &str) -> IVec {
        IVec::from(s)
    }

    fn ivec_to_str(iv: IVec) -> String {
        unsafe { String::from_utf8_unchecked(iv.to_vec()) }
    }
}

impl KvsEngine for SledAdapter {
    fn get(&self, key: &str) -> Result<Option<String>> {
        self.db
            .get(Self::ivec_from_str(key))
            .map(|x| x.map(Self::ivec_to_str))
            .context("Failed to get value.")
    }

    fn set(&self, key: &str, value: &str) -> Result<()> {
        let (ikey, ivalue) = (Self::ivec_from_str(key), Self::ivec_from_str(value));
        self.db.insert(ikey, ivalue).map(|_| ()).with_context(|| {
            format!(
                "Failed to insert value into Sled. key={}, value={}",
                key, value
            )
        })
    }

    fn remove(&self, key: &str) -> Result<()> {
        match self.db.remove(Self::ivec_from_str(key))? {
            Some(_) => Ok(()),
            None => bail!("Key: {} not found.", key),
        }
    }

    fn flush(&self) -> Result<()> {
        self.db.flush().map(|_| ()).context("Flush to disk.")
    }
}
