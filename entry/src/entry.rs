//! This module contains helper code for building `Entry` and `SequencedEntry`
//! from line protocol and the `DatabaseRules` configuration.

use std::sync::Arc;
use std::{
    convert::{TryFrom, TryInto},
    num::NonZeroU64,
};

use chrono::{DateTime, Utc};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use ouroboros::self_referencing;
use snafu::{OptionExt, ResultExt, Snafu};

use data_types::{
    database_rules::{Error as DataError, Partitioner, ShardId, Sharder},
    server_id::ServerId,
};
use influxdb_line_protocol::ParsedLine;
use internal_types::schema::{InfluxColumnType, InfluxFieldType};
use internal_types::write::{ColumnWrite, ColumnWriteValues, TableWrite};

use crate::entry_fb;
use arrow_util::bitset::{count_set_bits, negate_mask};
use hashbrown::HashMap;
use internal_types::write::line_protocol::{lines_to_table_writes, Error as LinesError, Options};
use std::borrow::Cow;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error generating partition key {}", source))]
    GeneratingPartitionKey { source: DataError },

    #[snafu(display("Error getting shard id {}", source))]
    GeneratingShardId { source: DataError },

    #[snafu(display("Error adding data to column {} on line {}", source, line_number))]
    ColumnError {
        line_number: usize,
        source: internal_types::write::builder::Error,
    },

    #[snafu(display(
        "table {} has column {} {} with new data on line {}",
        table,
        column,
        source,
        line_number
    ))]
    TableColumnTypeMismatch {
        table: String,
        column: String,
        line_number: usize,
        source: internal_types::write::builder::Error,
    },

    #[snafu(display("invalid flatbuffers: field {} is required", field))]
    FlatbufferFieldMissing { field: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Converts parsed line protocol into a collection of ShardedEntry with the
/// underlying flatbuffers bytes generated.
pub fn lines_to_sharded_entries<'a>(
    lines: impl IntoIterator<Item = &'a ParsedLine<'a>>,
    sharder: Option<&impl Sharder>,
    partitioner: &impl Partitioner,
) -> Result<Vec<ShardedEntry>> {
    let default_time = Utc::now();
    let options = Options {
        default_time: default_time.timestamp_nanos(),
        ..Default::default()
    };

    let lines = lines.into_iter().map(|line| -> Result<_> {
        let shard = sharder
            .map(|sharder| sharder.shard(&line).context(GeneratingShardId))
            .transpose()?;
        let partition = partitioner
            .partition_key(&line, &default_time)
            .context(GeneratingPartitionKey)?;
        Ok(((shard, partition), line))
    });

    let data = match lines_to_table_writes(lines, &options) {
        Ok(data) => data,
        Err(LinesError::ParseError { source, .. }) => return Err(source),
        Err(LinesError::ColumnError {
            line_number,
            source,
        }) => {
            return Err(Error::ColumnError {
                line_number,
                source,
            })
        }
    };

    // This shenanigans is temporary, sharding will soon be handled a level above in the server
    let mut shards: HashMap<_, HashMap<_, _>> = HashMap::new();
    for ((shard, partition_key), tables) in data {
        let t = shards.entry(shard).or_default();
        let ret = t.insert(partition_key, tables);
        assert!(ret.is_none(), "hashmap contained duplicates!")
    }

    Ok(shards
        .into_iter()
        .map(|(shard_id, partitioned)| {
            let entry = partitioned_writes_to_entry(partitioned);
            ShardedEntry { shard_id, entry }
        })
        .collect())
}

pub fn partitioned_writes_to_entry<'a>(
    partitions: HashMap<String, HashMap<Cow<'a, str>, TableWrite<'a>>>,
) -> Entry {
    let mut fbb = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

    let partition_writes = partitions
        .into_iter()
        .map(|(partition_key, tables)| build_partition_write(&mut fbb, partition_key, tables))
        .collect::<Vec<_>>();

    let partition_writes = fbb.create_vector(&partition_writes);

    let write_operations = entry_fb::WriteOperations::create(
        &mut fbb,
        &entry_fb::WriteOperationsArgs {
            partition_writes: Some(partition_writes),
        },
    );
    let entry = entry_fb::Entry::create(
        &mut fbb,
        &entry_fb::EntryArgs {
            operation_type: entry_fb::Operation::write,
            operation: Some(write_operations.as_union_value()),
        },
    );

    fbb.finish(entry, None);

    let (mut data, idx) = fbb.collapse();
    Entry::try_from(data.split_off(idx)).expect("Flatbuffer data just constructed should be valid")
}

fn build_partition_write<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    partition_key: String,
    tables: HashMap<Cow<'a, str>, TableWrite<'a>>,
) -> flatbuffers::WIPOffset<entry_fb::PartitionWrite<'a>> {
    let partition_key = fbb.create_string(&partition_key);
    let table_batches = tables
        .into_iter()
        .map(|(table_name, write)| build_table_write_batch(fbb, table_name.as_ref(), write))
        .collect::<Vec<_>>();

    let table_batches = fbb.create_vector(&table_batches);
    entry_fb::PartitionWrite::create(
        fbb,
        &entry_fb::PartitionWriteArgs {
            key: Some(partition_key),
            table_batches: Some(table_batches),
        },
    )
}

fn build_table_write_batch<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    table_name: &str,
    write: TableWrite<'a>,
) -> flatbuffers::WIPOffset<entry_fb::TableWriteBatch<'a>> {
    let columns = write
        .columns
        .into_iter()
        .map(|(column_name, write)| build_flatbuffer(column_name.as_ref(), write, fbb))
        .collect::<Vec<_>>();
    let columns = fbb.create_vector(&columns);

    let table_name = fbb.create_string(table_name);

    entry_fb::TableWriteBatch::create(
        fbb,
        &entry_fb::TableWriteBatchArgs {
            name: Some(table_name),
            columns: Some(columns),
        },
    )
}

/// Holds a shard id to the associated entry. If there is no ShardId, then
/// everything goes to the same place. This means a single entry will be
/// generated from a batch of line protocol.
#[derive(Debug)]
pub struct ShardedEntry {
    pub shard_id: Option<ShardId>,
    pub entry: Entry,
}

/// Wrapper type for the flatbuffer Entry struct. Has convenience methods for
/// iterating through the partitioned writes.
#[self_referencing]
#[derive(Debug, PartialEq)]
pub struct Entry {
    data: Vec<u8>,
    #[borrows(data)]
    #[covariant]
    fb: entry_fb::Entry<'this>,
}

impl Entry {
    /// Returns the Flatbuffers struct for the Entry
    pub fn fb(&self) -> &entry_fb::Entry<'_> {
        self.borrow_fb()
    }

    /// Returns the serialized bytes for the Entry
    pub fn data(&self) -> &[u8] {
        self.borrow_data()
    }

    pub fn partition_writes(&self) -> Option<Vec<PartitionWrite<'_>>> {
        match self.fb().operation_as_write().as_ref() {
            Some(w) => w
                .partition_writes()
                .as_ref()
                .map(|w| w.iter().map(|fb| PartitionWrite { fb }).collect::<Vec<_>>()),
            None => None,
        }
    }
}

impl TryFrom<Vec<u8>> for Entry {
    type Error = flatbuffers::InvalidFlatbuffer;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        EntryTryBuilder {
            data,
            fb_builder: |data| flatbuffers::root::<entry_fb::Entry<'_>>(data),
        }
        .try_build()
    }
}

impl From<Entry> for Vec<u8> {
    fn from(entry: Entry) -> Self {
        entry.into_heads().data
    }
}

/// Wrapper struct for the flatbuffers PartitionWrite. Has convenience methods
/// for iterating through the table batches.
#[derive(Debug)]
pub struct PartitionWrite<'a> {
    fb: entry_fb::PartitionWrite<'a>,
}

impl<'a> PartitionWrite<'a> {
    pub fn key(&self) -> &str {
        self.fb
            .key()
            .expect("key must be present in the flatbuffer PartitionWrite")
    }

    pub fn table_batches(&self) -> Vec<TableBatch<'_>> {
        match self.fb.table_batches().as_ref() {
            Some(batches) => batches
                .iter()
                .map(|fb| TableBatch { fb })
                .collect::<Vec<_>>(),
            None => vec![],
        }
    }
}

/// Wrapper struct for the flatbuffers TableBatch. Has convenience methods for
/// iterating through the data in columnar format.
#[derive(Debug)]
pub struct TableBatch<'a> {
    fb: entry_fb::TableWriteBatch<'a>,
}

impl<'a> TableBatch<'a> {
    pub fn name(&self) -> &'a str {
        self.fb
            .name()
            .expect("name must be present in flatbuffers TableWriteBatch")
    }

    pub fn columns(&self) -> Vec<Column<'a>> {
        match self.fb.columns().as_ref() {
            Some(columns) => {
                let row_count = self.row_count();
                columns
                    .iter()
                    .map(|fb| Column { fb, row_count })
                    .collect::<Vec<_>>()
            }
            None => vec![],
        }
    }

    pub fn row_count(&self) -> usize {
        if let Some(cols) = self.fb.columns() {
            if let Some(c) = cols.iter().next() {
                let null_count = match c.null_mask() {
                    Some(m) => m.iter().map(|b| b.count_ones() as usize).sum(),
                    None => 0,
                };

                let value_count = match c.values_type() {
                    entry_fb::ColumnValues::BoolValues => {
                        c.values_as_bool_values().unwrap().values().unwrap().len()
                    }
                    entry_fb::ColumnValues::U64Values => {
                        c.values_as_u64values().unwrap().values().unwrap().len()
                    }
                    entry_fb::ColumnValues::F64Values => {
                        c.values_as_f64values().unwrap().values().unwrap().len()
                    }
                    entry_fb::ColumnValues::I64Values => {
                        c.values_as_i64values().unwrap().values().unwrap().len()
                    }
                    entry_fb::ColumnValues::StringValues => {
                        c.values_as_string_values().unwrap().values().unwrap().len()
                    }
                    entry_fb::ColumnValues::BytesValues => {
                        c.values_as_bytes_values().unwrap().values().unwrap().len()
                    }
                    _ => panic!("invalid column flatbuffers"),
                };

                return value_count + null_count;
            }
        }

        0
    }
}

/// Wrapper struct for the flatbuffers Column. Has a convenience method to
/// return an iterator for the values in the column.
#[derive(Debug)]
pub struct Column<'a> {
    fb: entry_fb::Column<'a>,
    pub row_count: usize,
}

impl<'a> Column<'a> {
    pub fn name(&self) -> &'a str {
        self.fb
            .name()
            .expect("name must be present in flatbuffers Column")
    }

    pub fn inner(&self) -> &entry_fb::Column<'a> {
        &self.fb
    }

    pub fn influx_type(&self) -> InfluxColumnType {
        match (self.fb.values_type(), self.fb.logical_column_type()) {
            (entry_fb::ColumnValues::BoolValues, entry_fb::LogicalColumnType::Field) => {
                InfluxColumnType::Field(InfluxFieldType::Boolean)
            }
            (entry_fb::ColumnValues::U64Values, entry_fb::LogicalColumnType::Field) => {
                InfluxColumnType::Field(InfluxFieldType::UInteger)
            }
            (entry_fb::ColumnValues::F64Values, entry_fb::LogicalColumnType::Field) => {
                InfluxColumnType::Field(InfluxFieldType::Float)
            }
            (entry_fb::ColumnValues::I64Values, entry_fb::LogicalColumnType::Field) => {
                InfluxColumnType::Field(InfluxFieldType::Integer)
            }
            (entry_fb::ColumnValues::StringValues, entry_fb::LogicalColumnType::Tag) => {
                InfluxColumnType::Tag
            }
            (entry_fb::ColumnValues::StringValues, entry_fb::LogicalColumnType::Field) => {
                InfluxColumnType::Field(InfluxFieldType::String)
            }
            (entry_fb::ColumnValues::I64Values, entry_fb::LogicalColumnType::Time) => {
                InfluxColumnType::Timestamp
            }
            _ => unreachable!(),
        }
    }

    pub fn logical_type(&self) -> entry_fb::LogicalColumnType {
        self.fb.logical_column_type()
    }

    pub fn is_tag(&self) -> bool {
        self.fb.logical_column_type() == entry_fb::LogicalColumnType::Tag
    }

    pub fn is_field(&self) -> bool {
        self.fb.logical_column_type() == entry_fb::LogicalColumnType::Field
    }

    pub fn is_time(&self) -> bool {
        self.fb.logical_column_type() == entry_fb::LogicalColumnType::Time
    }
}

fn build_flatbuffer<'a>(
    column_name: &str,
    write: ColumnWrite<'a>,
    fbb: &mut FlatBufferBuilder<'a>,
) -> WIPOffset<entry_fb::Column<'a>> {
    let name = Some(fbb.create_string(column_name));
    let null_mask = if count_set_bits(&write.valid_mask) != write.row_count {
        let mut mask = write.valid_mask.into_owned();
        negate_mask(&mut mask, write.row_count);
        Some(fbb.create_vector_direct(&mask))
    } else {
        None
    };

    let values = match &write.values {
        ColumnWriteValues::String(values) => {
            let values = values
                .iter()
                .map(|v| fbb.create_string(v))
                .collect::<Vec<_>>();
            let values = fbb.create_vector(&values);
            entry_fb::StringValues::create(
                fbb,
                &entry_fb::StringValuesArgs {
                    values: Some(values),
                },
            )
            .as_union_value()
        }
        ColumnWriteValues::I64(values) => {
            let values = fbb.create_vector(&values);
            entry_fb::I64Values::create(
                fbb,
                &entry_fb::I64ValuesArgs {
                    values: Some(values),
                },
            )
            .as_union_value()
        }
        ColumnWriteValues::Bool(values) => {
            let values = fbb.create_vector(&values);
            entry_fb::BoolValues::create(
                fbb,
                &entry_fb::BoolValuesArgs {
                    values: Some(values),
                },
            )
            .as_union_value()
        }
        ColumnWriteValues::F64(values) => {
            let values = fbb.create_vector(&values);
            entry_fb::F64Values::create(
                fbb,
                &entry_fb::F64ValuesArgs {
                    values: Some(values),
                },
            )
            .as_union_value()
        }
        ColumnWriteValues::U64(values) => {
            let values = fbb.create_vector(&values);
            entry_fb::U64Values::create(
                fbb,
                &entry_fb::U64ValuesArgs {
                    values: Some(values),
                },
            )
            .as_union_value()
        }
        _ => unimplemented!(),
    };

    let (logical_column_type, values_type) = match write.influx_type {
        InfluxColumnType::Tag => (
            entry_fb::LogicalColumnType::Tag,
            entry_fb::ColumnValues::StringValues,
        ),
        InfluxColumnType::Timestamp => (
            entry_fb::LogicalColumnType::Time,
            entry_fb::ColumnValues::I64Values,
        ),
        InfluxColumnType::Field(field) => (
            entry_fb::LogicalColumnType::Field,
            match field {
                InfluxFieldType::Float => entry_fb::ColumnValues::F64Values,
                InfluxFieldType::Integer => entry_fb::ColumnValues::I64Values,
                InfluxFieldType::UInteger => entry_fb::ColumnValues::U64Values,
                InfluxFieldType::String => entry_fb::ColumnValues::StringValues,
                InfluxFieldType::Boolean => entry_fb::ColumnValues::BoolValues,
            },
        ),
    };

    entry_fb::Column::create(
        fbb,
        &entry_fb::ColumnArgs {
            name,
            logical_column_type,
            values_type,
            values: Some(values),
            null_mask,
        },
    )
}

#[derive(Debug, PartialOrd, PartialEq, Copy, Clone)]
pub struct ClockValue(NonZeroU64);

impl ClockValue {
    pub fn new(v: NonZeroU64) -> Self {
        Self(v)
    }

    pub fn get(&self) -> NonZeroU64 {
        self.0
    }

    pub fn get_u64(&self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for ClockValue {
    type Error = ClockValueError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(value)
            .map(Self)
            .context(ValueMayNotBeZero)
            .map_err(Into::into)
    }
}

#[derive(Debug, Snafu)]
pub struct ClockValueError(InnerClockValueError);

#[derive(Debug, Snafu)]
enum InnerClockValueError {
    #[snafu(display("Clock values must not be zero"))]
    ValueMayNotBeZero,
}

pub trait SequencedEntry: Send + Sync + std::fmt::Debug {
    fn partition_writes(&self) -> Option<Vec<PartitionWrite<'_>>>;

    fn fb(&self) -> &entry_fb::SequencedEntry<'_>;

    fn size(&self) -> usize;

    fn clock_value(&self) -> ClockValue {
        self.fb()
            .clock_value()
            .try_into()
            .expect("ClockValue should have been validated when this was built from flatbuffers")
    }

    fn server_id(&self) -> ServerId {
        self.fb()
            .server_id()
            .try_into()
            .expect("ServerId should have been validated when this was built from flatbuffers")
    }
}

#[self_referencing]
#[derive(Debug)]
pub struct OwnedSequencedEntry {
    data: Vec<u8>,
    #[borrows(data)]
    #[covariant]
    fb: entry_fb::SequencedEntry<'this>,
    #[borrows(data)]
    #[covariant]
    entry: Option<entry_fb::Entry<'this>>,
}

impl OwnedSequencedEntry {
    pub fn new_from_entry_bytes(
        clock_value: ClockValue,
        server_id: ServerId,
        entry_bytes: &[u8],
    ) -> Result<Self> {
        // The flatbuffer contains:
        //    1xu64 -> clock_value
        //    1xu32 -> server_id
        //    0?       -> entry (unused here)
        //    input   -> entry_bytes
        // The buffer also needs space for the flatbuffer vtable.
        const OVERHEAD: usize = 4 * std::mem::size_of::<u64>();
        let mut fbb = FlatBufferBuilder::new_with_capacity(entry_bytes.len() + OVERHEAD);

        let entry_bytes = fbb.create_vector_direct(entry_bytes);
        let sequenced_entry = entry_fb::SequencedEntry::create(
            &mut fbb,
            &entry_fb::SequencedEntryArgs {
                clock_value: clock_value.get_u64(),
                server_id: server_id.get_u32(),
                entry_bytes: Some(entry_bytes),
            },
        );

        fbb.finish(sequenced_entry, None);

        let (mut data, idx) = fbb.collapse();
        let sequenced_entry = Self::try_from(data.split_off(idx))
            .expect("Flatbuffer data just constructed should be valid");

        Ok(sequenced_entry)
    }
}

impl SequencedEntry for OwnedSequencedEntry {
    fn partition_writes(&self) -> Option<Vec<PartitionWrite<'_>>> {
        match self.borrow_entry().as_ref() {
            Some(e) => match e.operation_as_write().as_ref() {
                Some(w) => w
                    .partition_writes()
                    .as_ref()
                    .map(|w| w.iter().map(|fb| PartitionWrite { fb }).collect::<Vec<_>>()),
                None => None,
            },
            None => None,
        }
    }

    /// Returns the Flatbuffers struct for the SequencedEntry
    fn fb(&self) -> &entry_fb::SequencedEntry<'_> {
        self.borrow_fb()
    }

    fn size(&self) -> usize {
        self.borrow_data().len()
    }
}

#[derive(Debug, Snafu)]
pub enum SequencedEntryError {
    #[snafu(display("{}", source))]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },
    #[snafu(display("{}", source))]
    InvalidServerId {
        source: data_types::server_id::Error,
    },
    #[snafu(display("{}", source))]
    InvalidClockValue { source: ClockValueError },
    #[snafu(display("entry bytes not present in sequenced entry"))]
    EntryBytesMissing,
}

impl TryFrom<Vec<u8>> for OwnedSequencedEntry {
    type Error = SequencedEntryError;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        OwnedSequencedEntryTryBuilder {
            data,
            fb_builder: |data| {
                let fb = flatbuffers::root::<entry_fb::SequencedEntry<'_>>(data)
                    .context(InvalidFlatbuffer)?;

                // Raise an error now if the server ID is invalid so that `SequencedEntry`'s
                // `server_id` method can assume it has a valid `ServerId`
                TryInto::<ServerId>::try_into(fb.server_id()).context(InvalidServerId)?;

                // Raise an error now if the clock value is invalid so that `SequencedEntry`'s
                // `clock_value` method can assume it has a valid `ClockValue`
                TryInto::<ClockValue>::try_into(fb.clock_value()).context(InvalidClockValue)?;

                Ok(fb)
            },
            entry_builder: |data| match flatbuffers::root::<entry_fb::SequencedEntry<'_>>(data)
                .context(InvalidFlatbuffer)?
                .entry_bytes()
            {
                Some(entry_bytes) => Ok(Some(
                    flatbuffers::root::<entry_fb::Entry<'_>>(&entry_bytes)
                        .context(InvalidFlatbuffer)?,
                )),
                None => Ok(None),
            },
        }
        .try_build()
    }
}

#[derive(Debug)]
pub struct BorrowedSequencedEntry<'a> {
    fb: entry_fb::SequencedEntry<'a>,
    entry: Option<entry_fb::Entry<'a>>,
}

impl SequencedEntry for BorrowedSequencedEntry<'_> {
    fn partition_writes(&self) -> Option<Vec<PartitionWrite<'_>>> {
        match self.entry.as_ref() {
            Some(e) => match e.operation_as_write().as_ref() {
                Some(w) => w
                    .partition_writes()
                    .as_ref()
                    .map(|w| w.iter().map(|fb| PartitionWrite { fb }).collect::<Vec<_>>()),
                None => None,
            },
            None => None,
        }
    }

    fn fb(&self) -> &entry_fb::SequencedEntry<'_> {
        &self.fb
    }

    fn size(&self) -> usize {
        self.fb._tab.buf.len()
    }
}

#[self_referencing]
#[derive(Debug)]
pub struct Segment {
    data: Vec<u8>,
    #[borrows(data)]
    #[covariant]
    fb: entry_fb::Segment<'this>,
    #[borrows(data)]
    #[covariant]
    sequenced_entries: Vec<BorrowedSequencedEntry<'this>>,
}

impl Segment {
    pub fn new_from_entries(
        segment_id: u64,
        server_id: ServerId,
        clock_value: Option<ClockValue>,
        entries: &[Arc<dyn SequencedEntry>],
    ) -> Self {
        let mut fbb = FlatBufferBuilder::new_with_capacity(1024);

        let entries = entries
            .iter()
            .map(|e| {
                let entry_bytes = fbb.create_vector_direct(
                    e.fb()
                        .entry_bytes()
                        .expect("entry must be present in sequenced entry when initialized"),
                );

                entry_fb::SequencedEntry::create(
                    &mut fbb,
                    &entry_fb::SequencedEntryArgs {
                        server_id: e.server_id().get_u32(),
                        clock_value: e.clock_value().get_u64(),
                        entry_bytes: Some(entry_bytes),
                    },
                )
            })
            .collect::<Vec<_>>();

        let entries = fbb.create_vector(&entries);

        let segment = entry_fb::Segment::create(
            &mut fbb,
            &entry_fb::SegmentArgs {
                id: segment_id,
                server_id: server_id.get_u32(),
                consistency_high_water_clock: clock_value.map(|c| c.get_u64()).unwrap_or(0),
                entries: Some(entries),
            },
        );

        fbb.finish(segment, None);

        let (mut data, idx) = fbb.collapse();

        Self::try_from(data.split_off(idx))
            .expect("Flatbuffer data for sequenced entry just constructed should be valid")
    }

    /// Returns the Flatbuffers struct for the Segment
    pub fn fb(&self) -> &entry_fb::Segment<'_> {
        self.borrow_fb()
    }

    /// Returns the serialized bytes for the Segment
    pub fn data(&self) -> &[u8] {
        self.borrow_data()
    }

    pub fn consistency_high_water_clock(&self) -> Option<ClockValue> {
        self.fb().consistency_high_water_clock().try_into().ok()
    }

    pub fn server_id(&self) -> ServerId {
        self.fb()
            .server_id()
            .try_into()
            .expect("ServerId should have been validated when this was built from flatbuffers")
    }
}

impl TryFrom<Vec<u8>> for Segment {
    type Error = SequencedEntryError;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        SegmentTryBuilder {
            data,
            fb_builder: |data| {
                let fb =
                    flatbuffers::root::<entry_fb::Segment<'_>>(data).context(InvalidFlatbuffer)?;

                // Raise an error now if the server ID is invalid so that `SequencedEntry`'s
                // `server_id` method can assume it has a valid `ServerId`
                TryInto::<ServerId>::try_into(fb.server_id()).context(InvalidServerId)?;

                Ok(fb)
            },
            sequenced_entries_builder: |data| match flatbuffers::root::<entry_fb::Segment<'_>>(data)
                .context(InvalidFlatbuffer)?
                .entries()
            {
                Some(entries) => {
                    Ok(entries
                        .iter()
                        .map(|fb| {
                            // Raise an error now if the server ID is invalid so that `SequencedEntry`'s
                            // `server_id` method can assume it has a valid `ServerId`
                            TryInto::<ServerId>::try_into(fb.server_id())
                                .context(InvalidServerId)?;

                            // Raise an error now if the clock value is invalid so that `SequencedEntry`'s
                            // `clock_value` method can assume it has a valid `ClockValue`
                            TryInto::<ClockValue>::try_into(fb.clock_value())
                                .context(InvalidClockValue)?;

                            Ok(BorrowedSequencedEntry {
                                fb,
                                entry: Some(
                                    flatbuffers::root::<entry_fb::Entry<'_>>(
                                        &fb.entry_bytes().context(EntryBytesMissing)?,
                                    )
                                    .context(InvalidFlatbuffer)?,
                                ),
                            })
                        })
                        .collect::<Result<Vec<_>, Self::Error>>()?)
                }
                None => Ok(vec![]),
            },
        }
        .try_build()
    }
}

pub mod test_helpers {
    use chrono::TimeZone;

    use influxdb_line_protocol::parse_lines;

    use super::*;

    // An appropriate maximum size for batches of LP to be written into IOx. Using
    // test fixtures containing more than this many lines of LP will result in them
    // being written as multiple writes.
    const LP_BATCH_SIZE: usize = 10000;

    /// Converts the line protocol to a single `Entry` with a single shard and
    /// a single partition.
    pub fn lp_to_entry(lp: &str) -> Entry {
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        lines_to_sharded_entries(&lines, sharder(1).as_ref(), &hour_partitioner())
            .unwrap()
            .pop()
            .unwrap()
            .entry
    }

    /// Converts the line protocol to a collection of `Entry` with a single
    /// shard and a single partition, which is useful for testing when `lp` is
    /// large. Batches are sized according to LP_BATCH_SIZE.
    pub fn lp_to_entries(lp: &str) -> Vec<Entry> {
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        lines
            .chunks(LP_BATCH_SIZE)
            .map(|batch| {
                lines_to_sharded_entries(batch, sharder(1).as_ref(), &hour_partitioner())
                    .unwrap()
                    .pop()
                    .unwrap()
                    .entry
            })
            .collect::<Vec<_>>()
    }

    /// Sequences a given entry
    pub fn sequence_entry(
        server_id: u32,
        clock_value: u64,
        entry: &Entry
    ) -> OwnedSequencedEntry {
        let server_id = ServerId::try_from(server_id).unwrap();
        let clock_value = ClockValue::try_from(clock_value).unwrap();

        OwnedSequencedEntry::new_from_entry_bytes(clock_value, server_id, entry.data()).unwrap()
    }

    /// Converts the line protocol to a `SequencedEntry` with the given server id
    /// and clock value
    pub fn lp_to_sequenced_entry(
        lp: &str,
        server_id: u32,
        clock_value: u64,
    ) -> OwnedSequencedEntry {
        sequence_entry(server_id, clock_value, &lp_to_entry(lp))
    }

    /// Returns a test sharder that will assign shard ids from [0, count)
    /// incrementing for each line.
    pub fn sharder(count: ShardId) -> Option<TestSharder> {
        Some(TestSharder {
            count,
            n: std::cell::RefCell::new(0),
        })
    }

    // For each line passed to shard returns a shard id from [0, count) in order
    #[derive(Debug)]
    pub struct TestSharder {
        count: ShardId,
        n: std::cell::RefCell<ShardId>,
    }

    impl Sharder for TestSharder {
        fn shard(&self, _line: &ParsedLine<'_>) -> Result<ShardId, DataError> {
            let n = *self.n.borrow();
            self.n.replace(n + 1);
            Ok(n % self.count)
        }
    }

    /// Returns a test partitioner that will partition data by the hour
    pub fn hour_partitioner() -> HourPartitioner {
        HourPartitioner {}
    }

    /// Returns a test partitioner that will assign partition keys in the form
    /// key_# where # is replaced by a number `[0, count)` incrementing for
    /// each line.
    pub fn partitioner(count: u8) -> TestPartitioner {
        TestPartitioner {
            count,
            n: std::cell::RefCell::new(0),
        }
    }

    // For each line passed to partition_key returns a key with a number from
    // `[0, count)`
    #[derive(Debug)]
    pub struct TestPartitioner {
        count: u8,
        n: std::cell::RefCell<u8>,
    }

    impl Partitioner for TestPartitioner {
        fn partition_key(
            &self,
            _line: &ParsedLine<'_>,
            _default_time: &DateTime<Utc>,
        ) -> data_types::database_rules::Result<String> {
            let n = *self.n.borrow();
            self.n.replace(n + 1);
            Ok(format!("key_{}", n % self.count))
        }
    }

    // Partitions by the hour
    #[derive(Debug)]
    pub struct HourPartitioner {}

    impl Partitioner for HourPartitioner {
        fn partition_key(
            &self,
            line: &ParsedLine<'_>,
            default_time: &DateTime<Utc>,
        ) -> data_types::database_rules::Result<String> {
            const HOUR_FORMAT: &str = "%Y-%m-%dT%H";

            let key = match line.timestamp {
                Some(t) => Utc.timestamp_nanos(t).format(HOUR_FORMAT),
                None => default_time.format(HOUR_FORMAT),
            }
            .to_string();

            Ok(key)
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_util::bitset::iter_bits;
    use data_types::database_rules::NO_SHARD_CONFIG;
    use influxdb_line_protocol::parse_lines;
    use internal_types::write::TableWrite;

    use super::test_helpers::*;
    use super::*;
    use internal_types::schema::TIME_COLUMN_NAME;

    #[test]
    fn shards_lines() {
        let lp = vec![
            "cpu,host=a,region=west user=23.1,system=66.1 123",
            "mem,host=a,region=west used=23432 123",
            "foo bar=true 21",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let mut sharded_entries =
            lines_to_sharded_entries(&lines, sharder(2).as_ref(), &partitioner(1)).unwrap();
        sharded_entries.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));

        assert_eq!(sharded_entries.len(), 2);
        assert_eq!(sharded_entries[0].shard_id, Some(0));
        assert_eq!(sharded_entries[1].shard_id, Some(1));
    }

    #[test]
    fn no_shard_config() {
        let lp = vec![
            "cpu,host=a,region=west user=23.1,system=66.1 123",
            "mem,host=a,region=west used=23432 123",
            "foo bar=true 21",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, NO_SHARD_CONFIG, &partitioner(1)).unwrap();

        assert_eq!(sharded_entries.len(), 1);
        assert_eq!(sharded_entries[0].shard_id, None);
    }

    #[test]
    fn multiple_partitions() {
        let lp = vec![
            "cpu,host=a,region=west user=23.1,system=66.1 123",
            "mem,host=a,region=west used=23432 123",
            "asdf foo=\"bar\" 9999",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(2)).unwrap();

        let mut partition_writes = sharded_entries[0].entry.partition_writes().unwrap();
        partition_writes.sort_by(|a, b| a.key().cmp(b.key()));
        assert_eq!(partition_writes.len(), 2);
        assert_eq!(partition_writes[0].key(), "key_0");
        assert_eq!(partition_writes[1].key(), "key_1");
    }

    #[test]
    fn multiple_tables() {
        let lp = vec![
            "cpu val=1 55",
            "mem val=23 10",
            "cpu val=88 100",
            "disk foo=23.2 110",
            "mem val=55 111",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let partition_writes = sharded_entries[0].entry.partition_writes().unwrap();
        let mut table_batches = partition_writes[0].table_batches();
        table_batches.sort_by(|a, b| a.name().cmp(b.name()));

        assert_eq!(table_batches.len(), 3);
        assert_eq!(table_batches[0].name(), "cpu");
        assert_eq!(table_batches[1].name(), "disk");
        assert_eq!(table_batches[2].name(), "mem");
    }

    #[test]
    fn logical_column_types() {
        let lp = vec!["a,host=a val=23 983", "a,host=a,region=west val2=23.2 2343"].join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let partition_writes = sharded_entries[0].entry.partition_writes().unwrap();
        let table_batches = partition_writes[0].table_batches();
        let batch = &table_batches[0];

        let mut columns = batch.columns();
        columns.sort_by(|a, b| a.name().cmp(b.name()));

        assert_eq!(columns.len(), 5);

        assert_eq!(columns[0].name(), "host");
        assert_eq!(columns[0].logical_type(), entry_fb::LogicalColumnType::Tag);

        assert_eq!(columns[1].name(), "region");
        assert_eq!(columns[1].logical_type(), entry_fb::LogicalColumnType::Tag);

        assert_eq!(columns[2].name(), "time");
        assert_eq!(columns[2].logical_type(), entry_fb::LogicalColumnType::Time);

        assert_eq!(columns[3].name(), "val");
        assert_eq!(
            columns[3].logical_type(),
            entry_fb::LogicalColumnType::Field
        );

        assert_eq!(columns[4].name(), "val2");
        assert_eq!(
            columns[4].logical_type(),
            entry_fb::LogicalColumnType::Field
        );
    }

    #[test]
    fn columns_without_nulls() {
        let lp = vec![
            "a,host=a ival=23i,fval=1.2,uval=7u,sval=\"hi\",bval=true 1",
            "a,host=b ival=22i,fval=2.2,uval=1u,sval=\"world\",bval=false 2",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batches = partition_writes.first().unwrap().table_batches();
        let batch = table_batches.first().unwrap();

        assert_eq!(batch.row_count(), 2);

        let write: TableWrite = batch.into();
        let columns = write.columns;
        assert_eq!(columns.len(), 7);

        let col = &columns["bval"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(&valid, &[true, true]);
        assert_eq!(col.values.bool().unwrap(), &[true, false]);

        let col = &columns["fval"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(&valid, &[true, true]);
        assert_eq!(col.values.f64().unwrap(), &[1.2, 2.2]);

        let col = &columns["host"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(&valid, &[true, true]);
        assert_eq!(col.values.string().unwrap(), &["a", "b"]);

        let col = &columns["ival"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(&valid, &[true, true]);
        assert_eq!(col.values.i64().unwrap(), &[23, 22]);

        let col = &columns["sval"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(&valid, &[true, true]);
        assert_eq!(col.values.string().unwrap(), &["hi", "world"]);

        let col = &columns["time"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(&valid, &[true, true]);
        assert_eq!(col.values.i64().unwrap(), &[1, 2]);

        let col = &columns["uval"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(&valid, &[true, true]);
        assert_eq!(col.values.u64().unwrap(), &[7, 1]);
    }

    #[test]
    fn columns_with_nulls() {
        let lp = vec![
            "a,host=a val=23i 983",
            "a,host=a,region=west val2=23.2 2343",
            "a val=21i,bool=true,string=\"hello\" 222",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batches = partition_writes.first().unwrap().table_batches();
        let batch = table_batches.first().unwrap();

        assert_eq!(batch.row_count(), 3);

        let write: TableWrite = batch.into();
        let columns = write.columns;
        assert_eq!(columns.len(), 7);

        let col = &columns["bool"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(
            col.influx_type,
            InfluxColumnType::Field(InfluxFieldType::Boolean)
        );
        assert_eq!(&valid, &[false, false, true]);
        assert_eq!(col.values.bool().unwrap(), &[true]);

        let col = &columns["host"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(col.influx_type, InfluxColumnType::Tag);
        assert_eq!(&valid, &[true, true, false]);
        assert_eq!(col.values.string().unwrap(), &["a", "a"]);

        let col = &columns["region"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(col.influx_type, InfluxColumnType::Tag);
        assert_eq!(&valid, &[false, true, false]);
        assert_eq!(col.values.string().unwrap(), &["west"]);

        let col = &columns["string"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(
            col.influx_type,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        assert_eq!(&valid, &[false, false, true]);
        assert_eq!(col.values.string().unwrap(), &["hello"]);

        let col = &columns[TIME_COLUMN_NAME];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(col.influx_type, InfluxColumnType::Timestamp);
        assert_eq!(&valid, &[true, true, true]);
        assert_eq!(col.values.i64().unwrap(), &[983, 2343, 222]);

        let col = &columns["val"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(
            col.influx_type,
            InfluxColumnType::Field(InfluxFieldType::Integer)
        );
        assert_eq!(&valid, &[true, false, true]);
        assert_eq!(col.values.i64().unwrap(), &[23, 21]);

        let col = &columns["val2"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(
            col.influx_type,
            InfluxColumnType::Field(InfluxFieldType::Float)
        );
        assert_eq!(&valid, &[false, true, false]);
        assert_eq!(col.values.f64().unwrap(), &[23.2]);
    }

    #[test]
    fn row_count_edge_cases() {
        let lp = vec!["a val=1i 1"].join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();
        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batch = &partition_writes[0].table_batches()[0];
        assert_eq!(table_batch.row_count(), 1);
        let table: TableWrite = table_batch.into();

        let col = &table.columns["val"];
        assert_eq!(col.values.i64().unwrap(), &[1]);

        let lp = vec![
            "a val=1i 1",
            "a val=1i 2",
            "a val=1i 3",
            "a val=1i 4",
            "a val=1i 5",
            "a val=1i 6",
            "a val2=1i 7",
            "a val=1i 8",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();
        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batch = &partition_writes[0].table_batches()[0];
        assert_eq!(table_batch.row_count(), 8);
        let table: TableWrite = table_batch.into();

        let col = &table.columns["val"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(col.values.i64().unwrap(), &[1, 1, 1, 1, 1, 1, 1]);
        assert_eq!(&valid, &[true, true, true, true, true, true, false, true]);

        let lp = vec![
            "a val=1i 1",
            "a val=1i 2",
            "a val=1i 3",
            "a val=1i 4",
            "a val=1i 5",
            "a val=1i 6",
            "a val2=1i 7",
            "a val=1i 8",
            "a val=1i 9",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();
        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batch = &partition_writes[0].table_batches()[0];
        assert_eq!(table_batch.row_count(), 9);
        let table: TableWrite = table_batch.into();

        let col = &table.columns["val"];
        let valid = iter_bits(&col.valid_mask, col.row_count).collect::<Vec<_>>();
        assert_eq!(col.values.i64().unwrap(), &[1, 1, 1, 1, 1, 1, 1, 1]);
        assert_eq!(
            &valid,
            &[true, true, true, true, true, true, false, true, true]
        )
    }

    #[test]
    fn missing_times() {
        let lp = vec!["a val=1i", "a val=2i 123"].join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let t = Utc::now().timestamp_nanos();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let partition_writes = sharded_entries
            .first()
            .unwrap()
            .entry
            .partition_writes()
            .unwrap();
        let table_batch = &partition_writes[0].table_batches()[0];
        assert_eq!(table_batch.row_count(), 2);
        let table: TableWrite = table_batch.into();

        let col = &table.columns[TIME_COLUMN_NAME];
        let values = col.values.i64().unwrap();
        assert!(values[0] > t);
        assert_eq!(values[1], 123);
    }

    #[test]
    fn field_type_conflict() {
        let lp = vec!["a val=1i 1", "a val=2.1 123"].join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1));

        assert!(sharded_entries.is_err());
    }

    #[test]
    fn logical_type_conflict() {
        let lp = vec!["a,host=a val=1i 1", "a host=\"b\" 123"].join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1));

        assert!(sharded_entries.is_err());
    }

    #[test]
    fn sequenced_entry() {
        let lp = vec![
            "a,host=a val=23i 983",
            "a,host=a,region=west val2=23.2 2343",
            "a val=21i,bool=true,string=\"hello\" 222",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let entry_bytes = sharded_entries.first().unwrap().entry.data();
        let clock_value = ClockValue::try_from(23).unwrap();
        let server_id = ServerId::try_from(2).unwrap();
        let sequenced_entry =
            OwnedSequencedEntry::new_from_entry_bytes(clock_value, server_id, entry_bytes).unwrap();
        assert_eq!(sequenced_entry.clock_value(), clock_value);
        assert_eq!(sequenced_entry.server_id(), server_id);

        let partition_writes = sequenced_entry.partition_writes().unwrap();
        let table_batches = partition_writes.first().unwrap().table_batches();
        let batch = table_batches.first().unwrap();
        assert_eq!(batch.row_count(), 3);
    }

    #[test]
    fn validate_sequenced_entry_server_id() {
        let lp = vec![
            "a,host=a val=23i 983",
            "a,host=a,region=west val2=23.2 2343",
            "a val=21i,bool=true,string=\"hello\" 222",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let entry_bytes = sharded_entries.first().unwrap().entry.data();

        const OVERHEAD: usize = 4 * std::mem::size_of::<u64>();
        let mut fbb = FlatBufferBuilder::new_with_capacity(entry_bytes.len() + OVERHEAD);

        let entry_bytes = fbb.create_vector_direct(entry_bytes);
        let sequenced_entry = entry_fb::SequencedEntry::create(
            &mut fbb,
            &entry_fb::SequencedEntryArgs {
                clock_value: 3,
                server_id: 0, // <----- IMPORTANT PART this is invalid and should error
                entry_bytes: Some(entry_bytes),
            },
        );

        fbb.finish(sequenced_entry, None);

        let (mut data, idx) = fbb.collapse();
        let result = OwnedSequencedEntry::try_from(data.split_off(idx));

        assert!(
            matches!(result, Err(SequencedEntryError::InvalidServerId { .. })),
            "result was {:?}",
            result
        );
    }

    #[test]
    fn validate_sequenced_entry_clock_value() {
        let lp = vec![
            "a,host=a val=23i 983",
            "a,host=a,region=west val2=23.2 2343",
            "a val=21i,bool=true,string=\"hello\" 222",
        ]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        let sharded_entries =
            lines_to_sharded_entries(&lines, sharder(1).as_ref(), &partitioner(1)).unwrap();

        let entry_bytes = sharded_entries.first().unwrap().entry.data();

        const OVERHEAD: usize = 4 * std::mem::size_of::<u64>();
        let mut fbb = FlatBufferBuilder::new_with_capacity(entry_bytes.len() + OVERHEAD);

        let entry_bytes = fbb.create_vector_direct(entry_bytes);

        let sequenced_entry = entry_fb::SequencedEntry::create(
            &mut fbb,
            &entry_fb::SequencedEntryArgs {
                clock_value: 0, // <----- IMPORTANT PART this is invalid and should error
                server_id: 5,
                entry_bytes: Some(entry_bytes),
            },
        );

        fbb.finish(sequenced_entry, None);

        let (mut data, idx) = fbb.collapse();
        let result = OwnedSequencedEntry::try_from(data.split_off(idx));

        assert!(
            matches!(result, Err(SequencedEntryError::InvalidClockValue { .. })),
            "result was {:?}",
            result
        );
    }
}
