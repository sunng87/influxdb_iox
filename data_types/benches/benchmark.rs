use chrono::Utc;
use criterion::measurement::WallTime;
use criterion::{
    criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use data_types::data::{
    lines_to_replicated_write as lines_to_rw, ReplicatedWrite as ReplicatedWriteFB,
};
use data_types::database_rules::{DatabaseRules, PartitionTemplate, TemplatePart};
use generated_types::wal as wb;
use influxdb_line_protocol::{parse_lines, FieldValue as LineFieldValue, ParsedLine};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

const NEXT_ENTRY_NS: i64 = 1_000_000_000;
const STARTING_TIMESTAMP_NS: i64 = 0;

#[derive(Debug)]
struct Config {
    // total number of rows written in, regardless of the number of partitions
    line_count: i64,
    // this will be the number of write buffer entries per replicated write
    partition_count: usize,
    // the number of tables (measurements) in each replicated write
    table_count: usize,
    // the number of unique tag values for the tag in each replicated write
    tag_cardinality: usize,
}

impl Config {
    fn lines_per_partition(&self) -> i64 {
        self.line_count / self.partition_count as i64
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "lines: {} ({} per partition) | partitions: {} | tables per: {} | unique tag values per: {}",
            self.line_count,
            self.line_count / self.partition_count as i64,
            self.partition_count,
            self.table_count,
            self.tag_cardinality
        )
    }
}

fn lines_to_replicated_write(c: &mut Criterion) {
    run_group(
        "lines_to_replicated_write",
        c,
        |lines, rules, config, base_name, group| {
            let id = BenchmarkId::new(base_name, "fb");
            group.bench_with_input(id, &config, |b, config| {
                b.iter(|| {
                    let write = lines_to_rw(0, 0, &lines, &rules);
                    assert_eq!(write.entry_count(), config.partition_count);
                });
            });

            let id = BenchmarkId::new(base_name, "json");
            group.bench_with_input(id, &config, |b, config| {
                b.iter(|| {
                    let write = lines_to_rw_json(0, 0, &lines, &rules);
                    assert_eq!(write.entries.len(), config.partition_count);
                });
            });
        },
    );
}

fn replicated_write_into_bytes(c: &mut Criterion) {
    run_group(
        "replicated_write_into_bytes",
        c,
        |lines, rules, config, base_name, group| {
            let id = BenchmarkId::new(base_name, "fb");
            group.bench_with_input(id, &config, |b, config| {
                let write = lines_to_rw(0, 0, lines, rules);
                assert_eq!(write.entry_count(), config.partition_count);

                b.iter(|| {
                    let _ = write.bytes().len();
                });
            });

            let id = BenchmarkId::new(base_name, "json");
            group.bench_with_input(id, &config, |b, config| {
                let write = lines_to_rw_json(0, 0, lines, rules);
                assert_eq!(write.entries.len(), config.partition_count);

                b.iter(|| {
                    let _ = serde_json::to_vec(&write).unwrap();
                });
            });
        },
    );
}

// simulates the speed of marshalling the bytes into something like the mutable
// buffer or read buffer, which won't use the replicated write structure anyway
fn bytes_into_struct(c: &mut Criterion) {
    run_group(
        "bytes_into_struct",
        c,
        |lines, rules, config, base_name, group| {
            let id = BenchmarkId::new(base_name, "fb");

            group.bench_with_input(id, config, |b, config| {
                let write = lines_to_rw(0, 0, &lines, &rules);
                assert_eq!(write.entry_count(), config.partition_count);
                let data = write.bytes();

                b.iter(|| {
                    let mut db = Db::default();
                    db.deserialize_write(data);
                    assert_eq!(db.partition_count(), config.partition_count);
                    assert_eq!(db.row_count() as i64, config.line_count);
                    assert_eq!(db.measurement_count(), config.table_count);
                    assert_eq!(db.tag_cardinality(), config.tag_cardinality);
                });
            });

            let id = BenchmarkId::new(base_name, "json");

            group.bench_with_input(id, config, |b, config| {
                let write = lines_to_rw_json(0, 0, &lines, &rules);
                assert_eq!(write.entries.len(), config.partition_count);
                let data = serde_json::to_vec(&write).unwrap();

                b.iter(|| {
                    let db = json_bytes_to_struct(&data);
                    assert_eq!(db.partition_count(), config.partition_count);
                    assert_eq!(db.row_count() as i64, config.line_count);
                    assert_eq!(db.measurement_count(), config.table_count);
                    assert_eq!(db.tag_cardinality(), config.tag_cardinality);
                });
            });
        },
    );
}

fn run_group(
    group_name: &str,
    c: &mut Criterion,
    bench: impl Fn(&[ParsedLine], &DatabaseRules, &Config, &str, &mut BenchmarkGroup<WallTime>),
) {
    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(10));
    let rules = rules_with_time_partition();

    for partition_count in [1, 100].iter() {
        let config = Config {
            line_count: 1_000,
            partition_count: *partition_count,
            table_count: 1,
            tag_cardinality: 1,
        };
        group.throughput(Throughput::Elements(config.line_count as u64));

        let lp = create_lp(&config);
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let base_name = format!("partition count/{}", *partition_count);
        bench(&lines, &rules, &config, &base_name, &mut group);
    }

    for table_count in [1, 100].iter() {
        let config = Config {
            line_count: 1_000,
            partition_count: 1,
            table_count: *table_count,
            tag_cardinality: 1,
        };
        group.throughput(Throughput::Elements(config.line_count as u64));

        let lp = create_lp(&config);
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let base_name = format!("table count/{}", *table_count);
        bench(&lines, &rules, &config, &base_name, &mut group);
    }

    for tag_cardinality in [1, 100].iter() {
        let config = Config {
            line_count: 1_000,
            partition_count: 1,
            table_count: 1,
            tag_cardinality: *tag_cardinality,
        };
        group.throughput(Throughput::Elements(config.line_count as u64));

        let lp = create_lp(&config);
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let base_name = format!("tag cardinality/{}", *tag_cardinality);
        bench(&lines, &rules, &config, &base_name, &mut group);
    }

    group.finish();
}

// *****************************************************************************
// test structs and funcs for JSON. Intentionally different than the DB structs
fn lines_to_rw_json(
    writer: u32,
    sequence: u64,
    lines: &[ParsedLine],
    rules: &DatabaseRules,
) -> ReplicatedWrite {
    let default_time = Utc::now();
    let mut partitioned_entries = BTreeMap::new();

    for line in lines {
        let key = rules.partition_key(line, &default_time).unwrap();

        // get the batch for this partition
        let batch = partitioned_entries
            .entry(key)
            .or_insert_with(BTreeMap::new);

        // get the table for this line
        let table_name = line.series.measurement.as_str();
        let table_batch = batch.get_mut(table_name);
        let table_batch = match table_batch {
            Some(t) => t,
            None => {
                batch.insert(table_name.to_string(), vec![]);
                batch.get_mut(table_name).unwrap()
            }
        };

        // create the set of fields for this line
        let mut fields = vec![];
        if let Some(tags) = &line.series.tag_set {
            for (col, val) in tags {
                fields.push(FieldIn {
                    name: col.to_string(),
                    value: FieldValue::Tag(val.to_string()),
                });
            }
        }
        for (col, val) in &line.field_set {
            let v = match val {
                LineFieldValue::I64(v) => FieldValue::I64(*v),
                LineFieldValue::F64(v) => FieldValue::F64(*v),
                _ => unimplemented!(),
            };
            fields.push(FieldIn {
                name: col.to_string(),
                value: v,
            });
        }

        // add the row to the table
        table_batch.push(RowIn {
            field_values: fields,
        });
    }

    // convert the btrees to the actual entries
    let entries: Vec<_> = partitioned_entries
        .into_iter()
        .map(|(partition_key, tables)| {
            let table_batches: Vec<_> = tables
                .into_iter()
                .map(|(name, rows)| TableBatch { name, rows })
                .collect();

            BufferEntry {
                partition_key,
                table_batches,
            }
        })
        .collect();

    ReplicatedWrite {
        writer,
        sequence,
        entries,
    }
}

fn json_bytes_to_struct(data: &[u8]) -> Db {
    let write: ReplicatedWrite = serde_json::from_slice(data).unwrap();
    let mut db = Db::default();

    for entry in write.entries {
        if db.partitions.get(&entry.partition_key).is_none() {
            db.partitions
                .insert(entry.partition_key.clone(), Partition::default());
        }
        let partition = db.partitions.get_mut(&entry.partition_key).unwrap();

        for t in entry.table_batches {
            if partition.tables.get(&t.name).is_none() {
                let table = Table {
                    name: t.name.clone(),
                    ..Default::default()
                };
                partition.tables.insert(t.name.clone(), table);
            }
            let table = partition.tables.get_mut(&t.name).unwrap();

            for r in t.rows {
                let mut row = Row {
                    values: Vec::with_capacity(r.field_values.len()),
                };

                for v in r.field_values {
                    if partition.dict.get(&v.name).is_none() {
                        partition.dict.insert(v.name.clone(), partition.dict.len());
                    }
                    let column_index = *partition.dict.get(&v.name).unwrap();

                    let val = match v.value {
                        FieldValue::F64(v) => Value::F64(v),
                        FieldValue::I64(v) => Value::I64(v),
                        FieldValue::Tag(v) => {
                            if partition.dict.get(&v).is_none() {
                                partition.dict.insert(v.clone(), partition.dict.len());
                            }
                            let tag_index = *partition.dict.get(&v).unwrap();

                            Value::Tag(tag_index)
                        }
                    };

                    let column_value = ColumnValue {
                        column_name_index: column_index,
                        value: val,
                    };

                    row.values.push(column_value);
                }

                table.rows.push(row);
            }
        }
    }

    db
}

#[derive(Deserialize, Serialize)]
struct ReplicatedWrite {
    writer: u32,
    sequence: u64,
    entries: Vec<BufferEntry>,
}

#[derive(Deserialize, Serialize)]
struct BufferEntry {
    partition_key: String,
    table_batches: Vec<TableBatch>,
}

#[derive(Deserialize, Serialize)]
struct TableBatch {
    name: String,
    rows: Vec<RowIn>,
}

#[derive(Deserialize, Serialize)]
struct RowIn {
    field_values: Vec<FieldIn>,
}

#[derive(Deserialize, Serialize)]
struct FieldIn {
    name: String,
    value: FieldValue,
}

#[derive(Deserialize, Serialize)]
enum FieldValue {
    I64(i64),
    F64(f64),
    Tag(String),
}
// ****************************************************************************

#[derive(Default)]
struct Db {
    partitions: BTreeMap<String, Partition>,
}

impl Db {
    fn deserialize_write(&mut self, data: &[u8]) {
        let write = ReplicatedWriteFB::from(data);

        if let Some(batch) = write.write_buffer_batch() {
            if let Some(entries) = batch.entries() {
                for entry in entries {
                    let key = entry.partition_key().unwrap();

                    if self.partitions.get(key).is_none() {
                        self.partitions
                            .insert(key.to_string(), Partition::default());
                    }
                    let partition = self.partitions.get_mut(key).unwrap();

                    if let Some(tables) = entry.table_batches() {
                        for table_fb in tables {
                            let table_name = table_fb.name().unwrap();

                            if partition.tables.get(table_name).is_none() {
                                let table = Table {
                                    name: table_name.to_string(),
                                    ..Default::default()
                                };
                                partition.tables.insert(table_name.to_string(), table);
                            }
                            let table = partition.tables.get_mut(table_name).unwrap();

                            if let Some(rows) = table_fb.rows() {
                                for row_fb in rows {
                                    if let Some(values) = row_fb.values() {
                                        let mut row = Row {
                                            values: Vec::with_capacity(values.len()),
                                        };

                                        for value in values {
                                            let column_name = value.column().unwrap();

                                            if partition.dict.get(column_name).is_none() {
                                                partition.dict.insert(
                                                    column_name.to_string(),
                                                    partition.dict.len(),
                                                );
                                            }
                                            let column_index =
                                                *partition.dict.get(column_name).unwrap();

                                            let val = match value.value_type() {
                                                wb::ColumnValue::TagValue => {
                                                    let tag = value
                                                        .value_as_tag_value()
                                                        .unwrap()
                                                        .value()
                                                        .unwrap();

                                                    if partition.dict.get(tag).is_none() {
                                                        partition.dict.insert(
                                                            tag.to_string(),
                                                            partition.dict.len(),
                                                        );
                                                    }
                                                    let tag_index =
                                                        *partition.dict.get(tag).unwrap();

                                                    Value::Tag(tag_index)
                                                }
                                                wb::ColumnValue::F64Value => {
                                                    let val =
                                                        value.value_as_f64value().unwrap().value();

                                                    Value::F64(val)
                                                }
                                                wb::ColumnValue::I64Value => {
                                                    let val =
                                                        value.value_as_i64value().unwrap().value();

                                                    Value::I64(val)
                                                }
                                                _ => panic!("not supported!"),
                                            };

                                            let column_value = ColumnValue {
                                                column_name_index: column_index,
                                                value: val,
                                            };

                                            row.values.push(column_value);
                                        }

                                        table.rows.push(row);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    fn row_count(&self) -> usize {
        let mut count = 0;

        for p in self.partitions.values() {
            for t in p.tables.values() {
                count += t.rows.len();
            }
        }

        count
    }

    fn tag_cardinality(&self) -> usize {
        let mut tags = BTreeMap::new();

        for p in self.partitions.values() {
            for t in p.tables.values() {
                for r in &t.rows {
                    for v in &r.values {
                        if let Value::Tag(idx) = &v.value {
                            tags.insert(*idx, ());
                        }
                    }
                }
            }
        }

        tags.len()
    }

    fn measurement_count(&self) -> usize {
        let mut measurements = BTreeMap::new();

        for p in self.partitions.values() {
            for t in p.tables.values() {
                measurements.insert(t.name.to_string(), ());
            }
        }

        measurements.len()
    }
}

#[derive(Default)]
struct Partition {
    dict: BTreeMap<String, usize>,
    tables: BTreeMap<String, Table>,
}

#[derive(Default)]
struct Table {
    name: String,
    rows: Vec<Row>,
}

struct Row {
    values: Vec<ColumnValue>,
}

#[derive(Debug)]
struct ColumnValue {
    column_name_index: usize,
    value: Value,
}

#[derive(Debug)]
enum Value {
    I64(i64),
    F64(f64),
    Tag(usize),
}

fn create_lp(config: &Config) -> String {
    use std::fmt::Write;

    let mut s = String::new();

    let lines_per_partition = config.lines_per_partition();
    assert!(
        lines_per_partition >= config.table_count as i64,
        format!(
            "can't fit {} unique tables (measurements) into partition with {} rows",
            config.table_count, lines_per_partition
        )
    );
    assert!(
        lines_per_partition >= config.tag_cardinality as i64,
        format!(
            "can't fit {} unique tags into partition with {} rows",
            config.tag_cardinality, lines_per_partition
        )
    );

    for line in 0..config.line_count {
        let partition_number = line / lines_per_partition % config.partition_count as i64;
        let timestamp = STARTING_TIMESTAMP_NS + (partition_number * NEXT_ENTRY_NS);
        let mn = line % config.table_count as i64;
        let tn = line % config.tag_cardinality as i64;
        writeln!(
            s,
            "processes-{mn},host=my.awesome.computer.example.com-{tn} blocked=0i,zombies=0i,stopped=0i,running=42i,sleeping=999i,total=1041i,unknown=0i,idle=0i {timestamp}",
            mn = mn,
            tn = tn,
            timestamp = timestamp,
        ).unwrap();
    }

    s
}

fn rules_with_time_partition() -> DatabaseRules {
    let partition_template = PartitionTemplate {
        parts: vec![TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string())],
    };

    DatabaseRules {
        partition_template,
        ..Default::default()
    }
}

criterion_group!(
    benches,
    lines_to_replicated_write,
    replicated_write_into_bytes,
    bytes_into_struct,
);

criterion_main!(benches);
