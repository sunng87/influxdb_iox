use std::mem;
use std::sync::Arc;

use snafu::{ensure, Snafu};

use arrow::{
    array::{
        ArrayData, ArrayDataBuilder, ArrayRef, BooleanArray, DictionaryArray, Float64Array,
        Int64Array, TimestampNanosecondArray, UInt64Array,
    },
    datatypes::{DataType, Int32Type},
};
use arrow_util::bitset::{iter_bits, iter_set_positions, BitSet};
use arrow_util::string::PackedStringArray;
use data_types::partition_metadata::{IsNan, StatValues, Statistics};
use internal_types::schema::{InfluxColumnType, InfluxFieldType, TIME_DATA_TYPE};

use crate::dictionary::{Dictionary, DID, INVALID_DID};
use internal_types::write::{ColumnWrite, ColumnWriteValues};
use itertools::Either;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations)]
pub enum Error {
    #[snafu(display("Unable to insert {} type into a column of {}", inserted, existing,))]
    TypeMismatch {
        existing: InfluxColumnType,
        inserted: InfluxColumnType,
    },

    #[snafu(display(
        "Invalid null mask, expected to be {} bytes but was {}",
        expected_bytes,
        actual_bytes
    ))]
    InvalidNullMask {
        expected_bytes: usize,
        actual_bytes: usize,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Stores the actual data for columns in a chunk along with summary
/// statistics
#[derive(Debug)]
pub struct Column {
    influx_type: InfluxColumnType,
    valid: BitSet,
    data: ColumnData,
}

#[derive(Debug)]
pub enum ColumnData {
    F64(Vec<f64>, StatValues<f64>),
    I64(Vec<i64>, StatValues<i64>),
    U64(Vec<u64>, StatValues<u64>),
    String(PackedStringArray<i32>, StatValues<String>),
    Bool(BitSet, StatValues<bool>),
    Tag(Vec<DID>, StatValues<String>),
}

impl Column {
    pub fn new(row_count: usize, column_type: InfluxColumnType) -> Self {
        let mut valid = BitSet::new();
        valid.append_unset(row_count);

        let data = match column_type {
            InfluxColumnType::Field(InfluxFieldType::Boolean) => {
                let mut data = BitSet::new();
                data.append_unset(row_count);
                ColumnData::Bool(data, StatValues::default())
            }
            InfluxColumnType::Field(InfluxFieldType::UInteger) => {
                ColumnData::U64(vec![0; row_count], StatValues::default())
            }
            InfluxColumnType::Field(InfluxFieldType::Float) => {
                ColumnData::F64(vec![0.0; row_count], StatValues::default())
            }
            InfluxColumnType::Field(InfluxFieldType::Integer) | InfluxColumnType::Timestamp => {
                ColumnData::I64(vec![0; row_count], StatValues::default())
            }
            InfluxColumnType::Field(InfluxFieldType::String) => ColumnData::String(
                PackedStringArray::new_empty(row_count),
                StatValues::default(),
            ),
            InfluxColumnType::Tag => {
                ColumnData::Tag(vec![INVALID_DID; row_count], StatValues::default())
            }
        };

        Self {
            influx_type: column_type,
            valid,
            data,
        }
    }

    pub fn validate_schema(&self, influx_type: InfluxColumnType) -> Result<()> {
        ensure!(
            influx_type == self.influx_type,
            TypeMismatch {
                existing: self.influx_type,
                inserted: influx_type
            }
        );

        Ok(())
    }

    pub fn influx_type(&self) -> InfluxColumnType {
        self.influx_type
    }

    pub fn append(&mut self, write: &ColumnWrite<'_>, dictionary: &mut Dictionary) -> Result<()> {
        self.validate_schema(write.influx_type)?;

        let row_count = write.row_count;
        if row_count == 0 {
            return Ok(());
        }
        let mask = write.valid_mask.as_ref();

        match &mut self.data {
            ColumnData::Bool(col_data, stats) => {
                let data = match &write.values {
                    ColumnWriteValues::Bool(data) => Either::Left(data.iter().cloned()),
                    ColumnWriteValues::PackedBool(data) => {
                        Either::Right(iter_bits(data.as_ref(), row_count))
                    }
                    _ => unreachable!(), // TODO: Error Handling
                };

                let data_offset = col_data.len();
                col_data.append_unset(row_count);

                for (idx, value) in iter_set_positions(mask).zip(data) {
                    stats.update(&value);

                    if value {
                        col_data.set(data_offset + idx);
                    }
                }
            }
            ColumnData::U64(col_data, stats) => {
                let data = match &write.values {
                    ColumnWriteValues::U64(data) => data,
                    _ => unreachable!(), // TODO: Error Handling
                };

                handle_write(row_count, mask, data.iter(), col_data, stats);
            }
            ColumnData::F64(col_data, stats) => {
                let data = match &write.values {
                    ColumnWriteValues::F64(data) => data,
                    _ => unreachable!(), // TODO: Error Handling
                };

                handle_write(row_count, mask, data.iter(), col_data, stats);
            }
            ColumnData::I64(col_data, stats) => {
                let data = match &write.values {
                    ColumnWriteValues::I64(data) => data,
                    _ => unreachable!(), // TODO: Error Handling
                };

                handle_write(row_count, mask, data.iter(), col_data, stats);
            }
            ColumnData::String(col_data, stats) => {
                let data = match &write.values {
                    ColumnWriteValues::String(data) => data,
                    ColumnWriteValues::PackedString(_) => todo!(),
                    _ => unreachable!(), // TODO: Error Handling
                };

                let data_offset = col_data.len();

                let mut positions = iter_set_positions(&mask);
                for str in data.iter() {
                    let idx = positions.next().unwrap();
                    col_data.extend(data_offset + idx - col_data.len());
                    stats.update(str.as_ref());
                    col_data.append(str.as_ref());
                }

                col_data.extend(data_offset + row_count - col_data.len());
            }
            ColumnData::Tag(col_data, stats) => {
                let data = match &write.values {
                    ColumnWriteValues::String(data) => data,
                    ColumnWriteValues::PackedString(_) => todo!(),
                    ColumnWriteValues::Dictionary(_) => todo!(),
                    _ => unreachable!(), // TODO: Error Handling
                };

                let data_offset = col_data.len();
                col_data.resize(data_offset + row_count, INVALID_DID);

                for (idx, value) in iter_set_positions(mask).zip(data.as_ref()) {
                    stats.update(value.as_ref());
                    col_data[data_offset + idx] =
                        dictionary.lookup_value_or_insert(value.as_ref()).into();
                }
            }
        };

        self.valid.append_bits(row_count, mask);
        Ok(())
    }

    pub fn push_nulls_to_len(&mut self, len: usize) {
        if self.valid.len() == len {
            return;
        }
        assert!(len > self.valid.len(), "cannot shrink column");
        let delta = len - self.valid.len();
        self.valid.append_unset(delta);

        match &mut self.data {
            ColumnData::F64(data, _) => data.resize(len, 0.),
            ColumnData::I64(data, _) => data.resize(len, 0),
            ColumnData::U64(data, _) => data.resize(len, 0),
            ColumnData::String(data, _) => data.extend(delta),
            ColumnData::Bool(data, _) => data.append_unset(delta),
            ColumnData::Tag(data, _) => data.resize(len, INVALID_DID),
        }
    }

    pub fn len(&self) -> usize {
        self.valid.len()
    }

    pub fn stats(&self) -> Statistics {
        match &self.data {
            ColumnData::F64(_, stats) => Statistics::F64(stats.clone()),
            ColumnData::I64(_, stats) => Statistics::I64(stats.clone()),
            ColumnData::U64(_, stats) => Statistics::U64(stats.clone()),
            ColumnData::Bool(_, stats) => Statistics::Bool(stats.clone()),
            ColumnData::String(_, stats) | ColumnData::Tag(_, stats) => {
                Statistics::String(stats.clone())
            }
        }
    }

    /// The approximate memory size of the data in the column. Note that
    /// the space taken for the tag string values is represented in
    /// the dictionary size in the chunk that holds the table that has this
    /// column. The size returned here is only for their identifiers.
    pub fn size(&self) -> usize {
        let data_size = match &self.data {
            ColumnData::F64(v, stats) => mem::size_of::<f64>() * v.len() + mem::size_of_val(&stats),
            ColumnData::I64(v, stats) => mem::size_of::<i64>() * v.len() + mem::size_of_val(&stats),
            ColumnData::U64(v, stats) => mem::size_of::<u64>() * v.len() + mem::size_of_val(&stats),
            ColumnData::Bool(v, stats) => v.byte_len() + mem::size_of_val(&stats),
            ColumnData::Tag(v, stats) => mem::size_of::<DID>() * v.len() + mem::size_of_val(&stats),
            ColumnData::String(v, stats) => {
                let string_bytes_size = v.iter().fold(0, |acc, val| acc + val.len());
                let vec_pointer_sizes = mem::size_of::<String>() * v.len();
                string_bytes_size + vec_pointer_sizes + mem::size_of_val(&stats)
            }
        };
        data_size + self.valid.byte_len()
    }

    pub fn to_arrow(&self, dictionary: &ArrayData) -> Result<ArrayRef> {
        let nulls = self.valid.to_arrow();
        let data: ArrayRef = match &self.data {
            ColumnData::F64(data, _) => {
                let data = ArrayDataBuilder::new(DataType::Float64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .null_bit_buffer(nulls)
                    .build();
                Arc::new(Float64Array::from(data))
            }
            ColumnData::I64(data, _) => match self.influx_type {
                InfluxColumnType::Timestamp => {
                    let data = ArrayDataBuilder::new(TIME_DATA_TYPE())
                        .len(data.len())
                        .add_buffer(data.iter().cloned().collect())
                        .null_bit_buffer(nulls)
                        .build();
                    Arc::new(TimestampNanosecondArray::from(data))
                }
                InfluxColumnType::Field(InfluxFieldType::Integer) => {
                    let data = ArrayDataBuilder::new(DataType::Int64)
                        .len(data.len())
                        .add_buffer(data.iter().cloned().collect())
                        .null_bit_buffer(nulls)
                        .build();

                    Arc::new(Int64Array::from(data))
                }
                _ => unreachable!(),
            },
            ColumnData::U64(data, _) => {
                let data = ArrayDataBuilder::new(DataType::UInt64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .null_bit_buffer(nulls)
                    .build();
                Arc::new(UInt64Array::from(data))
            }
            ColumnData::String(data, _) => Arc::new(data.to_arrow()),
            ColumnData::Bool(data, _) => {
                let data = ArrayDataBuilder::new(DataType::Boolean)
                    .len(data.len())
                    .add_buffer(data.to_arrow())
                    .null_bit_buffer(nulls)
                    .build();
                Arc::new(BooleanArray::from(data))
            }
            ColumnData::Tag(data, _) => {
                let data = ArrayDataBuilder::new(DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                ))
                .len(data.len())
                .add_buffer(data.iter().cloned().collect())
                .null_bit_buffer(nulls)
                .add_child_data(dictionary.clone())
                .build();

                let array = DictionaryArray::<Int32Type>::from(data);

                Arc::new(array)
            }
        };

        assert_eq!(data.len(), self.len());

        Ok(data)
    }
}

/// Writes entry data into a column based on the valid mask
fn handle_write<'a, T, E>(
    row_count: usize,
    valid_mask: &[u8],
    write_data: E,
    col_data: &mut Vec<T>,
    stats: &mut StatValues<T>,
) where
    T: Clone + Default + PartialOrd + IsNan + 'static,
    E: Iterator<Item = &'a T> + ExactSizeIterator,
{
    let data_offset = col_data.len();
    col_data.resize(data_offset + row_count, Default::default());

    for (idx, value) in iter_set_positions(valid_mask).zip(write_data) {
        stats.update(value);
        col_data[data_offset + idx] = value.clone();
    }
}
