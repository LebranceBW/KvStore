use std::net::SocketAddrV4;
use std::process::exit;

use structopt::*;

use kvs::KvClient;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-client", version = env ! ("CARGO_PKG_VERSION"))]
#[allow(non_camel_case_types)]
enum ArgParser {
    #[structopt(about = "Insert a key-value pair to storage.")]
    set {
        #[structopt(about = "The key to insert.")]
        key: String,
        #[structopt(about = "The value to be inserted.")]
        value: String,
        #[structopt(short = "a", long = "addr", default_value = "127.0.0.1:4000")]
        address: SocketAddrV4,
    },
    #[structopt(about = "Get a record by the provided key.")]
    get {
        #[structopt(about = "The key of the value to take.")]
        key: String,
        #[structopt(short = "a", long = "addr", default_value = "127.0.0.1:4000")]
        address: SocketAddrV4,
    },
    #[structopt(about = "Remove an existing record by the provided key.")]
    rm {
        #[structopt(about = "The key of the value to remove.")]
        key: String,
        #[structopt(short = "a", long = "addr", default_value = "127.0.0.1:4000")]
        address: SocketAddrV4,
    },
}

#[allow(unused)]
fn main() {
    let matches = ArgParser::from_args();
    let reply = match matches {
        ArgParser::set {
            key,
            value,
            address,
        } => {
            KvClient::connect(address)
                .and_then(|mut client|
                    client.set(key, value))
        }
        ArgParser::get { key, address } => {
            KvClient::connect(address)
                .and_then(|mut client|
                    client.get(key))
        }
        ArgParser::rm { key, address } => {
            KvClient::connect(address)
                .and_then(|mut client|
                    client.remove(key))
        }
    };
    match reply {
        Ok(s) => println!("{}", s),
        Err(e) => {
            eprintln!("{}", e);
            exit(-1)
        }
    }
}
