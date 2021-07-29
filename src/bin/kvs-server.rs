use std::fs::{File, OpenOptions};
use std::io::Read;
use std::io::Write;
use std::net::{SocketAddrV4, ToSocketAddrs};
use std::path::PathBuf;
use std::str::FromStr;

use log::*;
use simple_logger::SimpleLogger;
use structopt::*;

use kvs::{EngineType, KvsEngine, KvServer, SledAdapter};
use kvs::engine::KvStore;
use kvs::thread_pool::{RayonThreadPool, ThreadPool};

const ENGINE_MARK_FILE: &'static str = ".engine_mark";

/// KVServer configuration.
#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server", version = env ! ("CARGO_PKG_VERSION"))]
struct ServerConfig {
    #[structopt(short = "a", long = "addr", default_value = "127.0.0.1:4000")]
    address: SocketAddrV4,
    #[structopt(short = "t", long = "engine", default_value = "kvs")]
    engine_type: EngineType,
}

fn main() {
    SimpleLogger::new()
        .with_level(LevelFilter::Debug)
        .init()
        .unwrap();
    let current_dir = std::env::current_dir().unwrap();
    let config = ServerConfig::from_args();
    // check directory.
    let (prev_engine, mut mark_fp) = read_from_mark_file(&current_dir);
    match prev_engine {
        Some(prev) => {
            info!("Retrieving last work. engine: {}", prev);
            if prev != config.engine_type {
                panic!(
                    "Mismatched engine type!, previous engine: {}, new engine: {}",
                    prev, config.engine_type
                )
            }
        }
        None => {
            write!(mark_fp, "{}", String::from(config.engine_type)).unwrap();
        }
    }
    info!(
        "Listened at {}, powered by {}, version: {}",
        config.address,
        config.engine_type,
        env!("CARGO_PKG_VERSION")
    );
    match &config.engine_type {
        EngineType::Kvs => run_with(
            KvStore::open(current_dir.as_path()).expect("Failed to create a server."),
            config.address,
        ),
        EngineType::Sled => run_with(
            SledAdapter::open(current_dir.as_path()).expect("Failed to create a sled engine."),
            config.address,
        ),
        _ => todo!(),
    }
}

fn run_with<T: KvsEngine>(engine: T, address: impl ToSocketAddrs) {
    let server = KvServer::new(engine, RayonThreadPool::new(4).unwrap(), address).unwrap();
    server.run()
}

fn read_from_mark_file(dir: &PathBuf) -> (Option<EngineType>, File) {
    let mut lock_fp = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(dir.join(ENGINE_MARK_FILE))
        .unwrap();
    let prev_engine = {
        let mut buf = String::new();
        lock_fp
            .read_to_string(&mut buf)
            .expect("Failed to read from mark file");
        EngineType::from_str(&buf).ok()
    };
    (prev_engine, lock_fp)
}
