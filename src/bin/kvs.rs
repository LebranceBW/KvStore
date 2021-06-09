use anyhow::Result;
use structopt::*;

use kvs::KvStore;

#[derive(Debug, StructOpt)]
#[structopt(name = env ! ("CARGO_PKG_NAME"), version = env ! ("CARGO_PKG_VERSION"))]
#[allow(non_camel_case_types)]
enum ArgParser {
    #[structopt(about = "Insert a key-value pair to storage.")]
    set {
        #[structopt(about = "The key to insert.")]
        key: String,
        #[structopt(about = "The value to be inserted.")]
        value: String,
    },
    #[structopt(about = "Get a record by the provided key.")]
    get {
        #[structopt(about = "The key of the value to take.")]
        key: String,
    },
    #[structopt(about = "Remove an existing record by the provided key.")]
    rm {
        #[structopt(about = "The key of the value to remove.")]
        key: String,
    },
    #[structopt(about = "List all key-value pairs.")]
    list,
}

#[allow(unused)]
fn main() -> Result<()> {
    let matches = ArgParser::from_args();
    match matches {
        ArgParser::set { key, value } => KvStore::open("./")?.set(&key, &value),
        ArgParser::get { key } => {
            let logged = key.clone();
            let value = KvStore::open("./")?.get(&key)?;
            match &value {
                Some(val) => println!("{}", val),
                None => println!("Key: {} not found", logged),
            };
            Ok(())
        }
        ArgParser::rm { key } => KvStore::open("./")?.remove(&key),
        ArgParser::list => {
            todo!()
            // KvStore::open("./")?
            //     .list()?
            //     .iter()
            //     .for_each(|(key, value)| println!("{}: {}", key, value));
            // Ok(())
        }
    }
}
