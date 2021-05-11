use std::{convert::TryFrom, io::Read};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flate2::read::GzDecoder;

use data_types::server_id::ServerId;
use internal_types::write::line_protocol::lp_to_table_writes;
use internal_types::write::TableWrite;
use mutable_buffer::chunk::Chunk;
use tracker::MemRegistry;

#[inline]
fn write_chunk(count: usize, writes: &[TableWrite<'_>]) {
    let mut chunk = Chunk::new(0, &MemRegistry::new());

    for _ in 0..count {
        for write in writes {
            chunk.write_table_batch(&write).unwrap();
        }
    }
}

fn load_lp() -> String {
    let raw = include_bytes!("../../tests/fixtures/lineproto/tag_values.lp.gz");
    let mut gz = GzDecoder::new(&raw[..]);
    let mut lp = String::new();
    gz.read_to_string(&mut lp).unwrap();
    lp
}

pub fn write_mb(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_mb");
    let lp = load_lp();
    let batches = lp_to_table_writes(&lp, &Default::default()).unwrap();

    for count in &[1, 2, 3, 4, 5] {
        group.bench_function(BenchmarkId::from_parameter(count), |b| {
            b.iter(|| write_chunk(*count, &entries));
        });
    }
    group.finish();
}

criterion_group!(benches, write_mb);
criterion_main!(benches);
