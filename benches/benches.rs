use bencher::Bencher;
use bencher::{benchmark_group, benchmark_main};
use lazy_static::lazy_static;
use rand::distributions::Alphanumeric;
use rand::Rng;
use tempfile::TempDir;

use kvs::switch_engine;
use kvs::*;

lazy_static! {
    static ref TEST_SET: Vec<(String, String)> = {
        let mut rng = rand::thread_rng();
        (0..100)
            .map(move |_| {
                let key = random_string(rng.gen_range(1..100000));
                let value = random_string(rng.gen_range(1..100000));
                (key, value)
            })
            .collect::<Vec<_>>()
    };
}

fn random_string(len: usize) -> String {
    let rng = rand::thread_rng();
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn kvs_write(bench: &mut Bencher) {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = switch_engine(EngineType::Kvs, temp_dir.path()).unwrap();
    bench.iter(|| {
        for (key, value) in TEST_SET.iter() {
            engine.set(key, value).unwrap();
        }
    });
}

fn sled_read(bench: &mut Bencher) {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = switch_engine(EngineType::Sled, temp_dir.path()).unwrap();
    TEST_SET.iter().for_each(|(k, v)| engine.set(k, v).unwrap());
    bench.iter(|| {
        TEST_SET.iter().for_each(|(k, v)| {
            let stored_v = engine.get(k).unwrap().unwrap();
            assert_eq!(&stored_v, v)
        })
    });
}

fn kvs_read(bench: &mut Bencher) {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = switch_engine(EngineType::Kvs, temp_dir.path()).unwrap();
    TEST_SET.iter().for_each(|(k, v)| engine.set(k, v).unwrap());
    bench.iter(|| {
        TEST_SET.iter().for_each(|(k, v)| {
            let stored_v = engine.get(k).unwrap().unwrap();
            assert_eq!(&stored_v, v)
        })
    });
}

fn sled_write(bench: &mut Bencher) {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = switch_engine(EngineType::Sled, temp_dir.path()).unwrap();
    bench.iter(|| {
        for (key, value) in TEST_SET.iter() {
            engine.set(key, value).unwrap();
        }
    });
}

benchmark_group!(benches, kvs_read, sled_read, kvs_write, sled_write);
benchmark_main!(benches);
