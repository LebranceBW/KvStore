use std::process::exit;

use structopt::*;

#[derive(Debug, StructOpt)]
#[structopt(name=env!("CARGO_PKG_NAME"), version=env!("CARGO_PKG_VERSION"))]
#[allow(non_camel_case_types)]
enum ArgParser {
    #[structopt(about = "Insert a key-value pair to storage.")]
    set{
        #[structopt(about = "The key to insert.")]
        key: String,
        #[structopt(about = "The value to be inserted.")]
        value: String
    },
    #[structopt(about = "Get a record by the provided key.")]
    get{
        #[structopt(about = "The key of the value to take.")]
        key:String,
    },
    #[structopt(about = "Remove an existing record by the provided key.")]
    rm {
        #[structopt(about = "The key of the value to remove.")]
        key:String,
    }
}

#[allow(unused)]
fn main() {
    let matches = ArgParser::from_args();
    match matches {
        ArgParser::set{key, value} => eprintln!("unimplemented"),
        ArgParser::get{key} => eprintln!("unimplemented"),
        ArgParser::rm{key} => eprintln!("unimplemented"),
        _ => unreachable!(),
    };
    exit(-1);
}