use criterion::{criterion_group, criterion_main};

mod engine {
    use criterion::{Bencher, Criterion};
    use lazy_static::lazy_static;
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use tempfile::TempDir;

    use kvs::engine::KvStore;
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
        let engine = KvStore::open(temp_dir.path()).unwrap();
        bench.iter(|| {
            for (key, value) in TEST_SET.iter() {
                engine.set(key, value).unwrap();
            }
        });
    }

    fn sled_read(bench: &mut Bencher) {
        let temp_dir = TempDir::new().unwrap();
        let engine = SledAdapter::open(temp_dir.path()).unwrap();
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
        let engine = KvStore::open(temp_dir.path()).unwrap();
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
        let engine = SledAdapter::open(temp_dir.path()).unwrap();
        bench.iter(|| {
            for (key, value) in TEST_SET.iter() {
                engine.set(key, value).unwrap();
            }
        });
    }

    pub fn engine_test_suite(bencher: &mut Criterion) {
        let mut group = bencher.benchmark_group("Engine tests");
        let _test_val = &TEST_SET;
        group.bench_function("sled-write", |b| sled_write(b));
        group.bench_function("sled-read", |b| sled_read(b));
        group.bench_function("kvs-write", |b| kvs_write(b));
        group.bench_function("kvs-read", |b| kvs_read(b));
        group.finish();
    }
}

mod thread_pool {
    use criterion::Criterion;
    use tempfile::TempDir;

    use kvs::thread_pool::ThreadPool;
    use kvs::{KvServer, SledAdapter};

    pub fn suite_main(ct: &mut Criterion) {
        let _group = ct.benchmark_group("Write_test");
    }

    fn write_queued_kvstore<T: ThreadPool>(pool: T) {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let _server = KvServer::new(
            SledAdapter::open(temp_dir.path()).unwrap(),
            pool,
            format!("127.0.0.1:8888"),
        );

        for _ in 0..num_cpus::get() {}
    }
}
criterion_group!(benches, engine::engine_test_suite, thread_pool::suite_main);
criterion_main!(benches);
