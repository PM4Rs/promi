use criterion::{criterion_group, criterion_main, Criterion};
use promi::stream::{consume, xes};
use std::fs;
use std::io;
use std::path;

fn xes_read() {
    let path = path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("static")
        .join("chapter_5")
        .join("L1.xes");
    let f = io::BufReader::new(fs::File::open(&path).unwrap());
    let mut reader = xes::XesReader::from(f);

    consume(&mut reader).unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("xes_read", |b| b.iter(|| xes_read()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);