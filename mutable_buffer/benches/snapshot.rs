use std::{convert::TryFrom, io::Read};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use data_types::server_id::ServerId;
use flate2::read::GzDecoder;
use internal_types::write::line_protocol::lp_to_table_writes;
use mutable_buffer::chunk::Chunk;
use tracker::MemRegistry;

#[inline]
fn snapshot_chunk(chunk: &Chunk) {
    let _ = chunk.snapshot();
}

fn chunk(count: usize) -> Chunk {
    let mut chunk = Chunk::new(0, &MemRegistry::new());

    let raw = include_bytes!("../../tests/fixtures/lineproto/tag_values.lp.gz");
    let mut gz = GzDecoder::new(&raw[..]);
    let mut lp = String::new();
    gz.read_to_string(&mut lp).unwrap();

    let options = Default::default();

    for _ in 0..count {
        for write in lp_to_table_writes(&lp, &options).unwrap() {
            chunk.write_table_batch(&write).unwrap();
        }
    }

    chunk
}

pub fn snapshot_mb(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_mb");
    for count in &[1, 2, 3, 4, 5] {
        let chunk = chunk(*count as _);
        group.bench_function(BenchmarkId::from_parameter(count), |b| {
            b.iter(|| snapshot_chunk(&chunk));
        });
    }
    group.finish();
}

criterion_group!(benches, snapshot_mb);
criterion_main!(benches);
