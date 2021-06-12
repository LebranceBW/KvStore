use anyhow::Result;
use log::LevelFilter;
use simple_logger::SimpleLogger;
use structopt::*;

use kvs::KvServer;
use kvs::ServerConfig;

fn main() -> Result<()> {
    SimpleLogger::new().with_level(LevelFilter::Debug).init()?;
    let config = ServerConfig::from_args();
    let server = KvServer::new(config)?;
    server.run()
}
