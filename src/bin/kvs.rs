use std::process::exit;

use clap::{App, SubCommand};

fn get_matcher() -> App<'static, 'static> {
    App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg_from_usage("<KEY> 'The key to store.'")
                .arg_from_usage("<VALUE> 'The value to store.'"),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the value by a key")
                .arg_from_usage("<KEY> 'The key to search'"),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a record by a key")
                .arg_from_usage("<KEY> 'The key to remove'"),
        )
}

fn main() {
    let matches = get_matcher().get_matches();
    match matches.subcommand() {
        ("set", Some(_sub_arg)) => eprintln!("unimplemented"),
        ("get", Some(_sub_arg)) => eprintln!("unimplemented"),
        ("rm", Some(_sub_arg)) => eprintln!("unimplemented"),
        _ => unreachable!(),
    };
    exit(-1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn clap_parse_set_value() {
        let matcher = get_matcher();
        let case = matcher.get_matches_from(vec![env!("CARGO_PKG_NAME"), "set", "key1", "value1"]);
        assert_eq!(case.subcommand().0, ("set"));
        let args = case.subcommand().1.unwrap();
        assert_eq!(args.value_of("KEY"), Some("key1"));
        assert_eq!(args.value_of("VALUE"), Some("value1"));
    }

    #[test]
    #[should_panic]
    pub fn clap_parse_bad_set() {
        let matcher = get_matcher();
        matcher
            .get_matches_from_safe(vec![env!("CARGO_PKG_NAME"), "set", "key1"])
            .unwrap();
    }

    #[test]
    pub fn clap_parse_rm() {
        let matcher = get_matcher();
        let case = matcher
            .get_matches_from_safe(vec![env!("CARGO_PKG_NAME"), "rm", "key1"])
            .unwrap();
        assert_eq!(case.subcommand().0, ("rm"));
        assert_eq!(case.subcommand().1.unwrap().value_of("KEY"), Some("key1"));
    }

    #[test]
    pub fn clap_parse_get_value() {
        let matcher = get_matcher();
        let case = matcher
            .get_matches_from_safe(vec![env!("CARGO_PKG_NAME"), "get", "key1"])
            .unwrap();
        assert_eq!(case.subcommand().0, ("get"));
        assert_eq!(case.subcommand().1.unwrap().value_of("KEY"), Some("key1"));
    }
}
