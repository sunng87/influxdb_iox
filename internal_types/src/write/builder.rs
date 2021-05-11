use snafu::{ensure, Snafu};

use arrow_util::bitset::BitSet;
use arrow_util::dictionary::StringDictionary;
use arrow_util::string::PackedStringArray;

use crate::schema::{InfluxColumnType, InfluxFieldType};
use crate::write::{ColumnWrite, ColumnWriteValues, Dictionary, PackedStrings};
use std::borrow::Cow;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("influx type mismatch: expected {} but got {}", expected, new))]
    InfluxTypeMismatch {
        new: InfluxColumnType,
        expected: InfluxColumnType,
    },

    #[snafu(display("physical type mismatch: expected {} but got {}", expected, new))]
    PhysicalTypeMismatch {
        new: &'static str,
        expected: &'static str,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ColumnWriteBuilder<'a> {
    influx_type: InfluxColumnType,
    valid_mask: BitSet,
    values: ColumnValues<'a>,
}

impl<'a> ColumnWriteBuilder<'a> {
    pub fn new_tag_column(dictionary: bool, packed: bool, initial_nulls: usize) -> Self {
        Self {
            influx_type: InfluxColumnType::Tag,
            valid_mask: BitSet::new_with_unset(initial_nulls),
            values: ColumnValues::new_str(dictionary, packed),
        }
    }

    pub fn new_string_column(dictionary: bool, packed: bool, initial_nulls: usize) -> Self {
        Self {
            influx_type: InfluxColumnType::Field(InfluxFieldType::String),
            valid_mask: BitSet::new_with_unset(initial_nulls),
            values: ColumnValues::new_str(dictionary, packed),
        }
    }

    pub fn new_time_column(initial_nulls: usize) -> Self {
        Self {
            influx_type: InfluxColumnType::Timestamp,
            valid_mask: BitSet::new_with_unset(initial_nulls),
            values: ColumnValues::I64(Vec::new()),
        }
    }

    pub fn new_bool_column(packed: bool, initial_nulls: usize) -> Self {
        let values = match packed {
            true => ColumnValues::PackedBool(BitSet::new()),
            false => ColumnValues::Bool(Vec::new()),
        };

        Self {
            influx_type: InfluxColumnType::Field(InfluxFieldType::Boolean),
            valid_mask: BitSet::new_with_unset(initial_nulls),
            values,
        }
    }

    pub fn new_u64_column(initial_nulls: usize) -> Self {
        Self {
            influx_type: InfluxColumnType::Field(InfluxFieldType::UInteger),
            valid_mask: BitSet::new_with_unset(initial_nulls),
            values: ColumnValues::U64(Vec::new()),
        }
    }

    pub fn new_f64_column(initial_nulls: usize) -> Self {
        Self {
            influx_type: InfluxColumnType::Field(InfluxFieldType::Float),
            valid_mask: BitSet::new_with_unset(initial_nulls),
            values: ColumnValues::F64(Vec::new()),
        }
    }

    pub fn new_i64_column(initial_nulls: usize) -> Self {
        Self {
            influx_type: InfluxColumnType::Field(InfluxFieldType::Integer),
            valid_mask: BitSet::new_with_unset(initial_nulls),
            values: ColumnValues::I64(Vec::new()),
        }
    }

    // ensures there are at least as many rows (or nulls) to idx
    pub fn null_to_idx(&mut self, idx: usize) {
        if idx > self.valid_mask.len() {
            self.valid_mask.append_unset(idx - self.valid_mask.len())
        }
    }

    pub fn push_tag(&mut self, value: Cow<'a, str>) -> Result<()> {
        self.verify_type(InfluxColumnType::Tag)?;
        self.values.push_str(value)?;
        self.valid_mask.push(true);
        Ok(())
    }

    pub fn push_string(&mut self, value: Cow<'a, str>) -> Result<()> {
        self.verify_type(InfluxColumnType::Field(InfluxFieldType::String))?;
        self.values.push_str(value)?;
        self.valid_mask.push(true);
        Ok(())
    }

    pub fn push_time(&mut self, value: i64) -> Result<()> {
        self.verify_type(InfluxColumnType::Timestamp)?;
        self.values.push_i64(value)?;
        self.valid_mask.push(true);
        Ok(())
    }

    pub fn push_bool(&mut self, value: bool) -> Result<()> {
        self.verify_type(InfluxColumnType::Field(InfluxFieldType::Boolean))?;
        self.values.push_bool(value)?;
        self.valid_mask.push(true);
        Ok(())
    }

    pub fn push_u64(&mut self, value: u64) -> Result<()> {
        self.verify_type(InfluxColumnType::Field(InfluxFieldType::UInteger))?;
        self.values.push_u64(value)?;
        self.valid_mask.push(true);
        Ok(())
    }

    pub fn push_f64(&mut self, value: f64) -> Result<()> {
        self.verify_type(InfluxColumnType::Field(InfluxFieldType::Float))?;
        self.values.push_f64(value)?;
        self.valid_mask.push(true);
        Ok(())
    }

    pub fn push_i64(&mut self, value: i64) -> Result<()> {
        self.verify_type(InfluxColumnType::Field(InfluxFieldType::Integer))?;
        self.values.push_i64(value)?;
        self.valid_mask.push(true);
        Ok(())
    }

    pub fn verify_type(&self, influx_type: InfluxColumnType) -> Result<()> {
        ensure!(
            self.influx_type == influx_type,
            InfluxTypeMismatch {
                new: influx_type,
                expected: self.influx_type,
            }
        );
        Ok(())
    }

    pub fn build(self) -> ColumnWrite<'a> {
        ColumnWrite {
            row_count: self.valid_mask.len(),
            influx_type: self.influx_type,
            valid_mask: self.valid_mask.take_bytes().into(),
            values: self.values.into(),
        }
    }
}

#[derive(Debug)]
enum ColumnValues<'a> {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
    String(Vec<Cow<'a, str>>),
    PackedString(PackedStringArray<u16>),
    Dictionary(StringDictionary<u16>, Vec<u16>),
    PackedBool(BitSet),
    Bool(Vec<bool>),
}

impl<'a> From<ColumnValues<'a>> for ColumnWriteValues<'a> {
    fn from(raw: ColumnValues<'a>) -> Self {
        match raw {
            ColumnValues::F64(data) => Self::F64(data.into()),
            ColumnValues::I64(data) => Self::I64(data.into()),
            ColumnValues::U64(data) => Self::U64(data.into()),
            ColumnValues::String(data) => Self::String(data.into()),
            ColumnValues::PackedString(data) => {
                let (indexes, values) = data.into_inner();
                Self::PackedString(PackedStrings {
                    indexes: indexes.into(),
                    values: values.into(),
                })
            }
            ColumnValues::Dictionary(dictionary, keys) => {
                let (indexes, values) = dictionary.into_inner().into_inner();
                Self::Dictionary(Dictionary {
                    keys: keys.into(),
                    values: PackedStrings {
                        indexes: indexes.into(),
                        values: values.into(),
                    },
                })
            }
            ColumnValues::PackedBool(data) => Self::PackedBool(data.take_bytes().into()),
            ColumnValues::Bool(data) => Self::Bool(data.into()),
        }
    }
}

impl<'a> ColumnValues<'a> {
    fn new_str(dictionary: bool, packed: bool) -> Self {
        match (dictionary, packed) {
            (true, _) => ColumnValues::Dictionary(StringDictionary::new(), Vec::new()),
            (false, true) => ColumnValues::PackedString(PackedStringArray::new()),
            (false, false) => ColumnValues::String(Vec::new()),
        }
    }

    fn push_str(&mut self, value: Cow<'a, str>) -> Result<()> {
        match self {
            ColumnValues::String(data) => {
                data.push(value);
            }
            ColumnValues::PackedString(data) => {
                data.append(value.as_ref());
            }
            ColumnValues::Dictionary(dictionary, values) => {
                let id = dictionary.lookup_value_or_insert(value.as_ref());
                values.push(id);
            }
            _ => PhysicalTypeMismatch {
                new: "str",
                expected: self.type_description(),
            }
            .fail()?,
        }
        Ok(())
    }

    fn push_i64(&mut self, value: i64) -> Result<()> {
        match self {
            ColumnValues::I64(data) => data.push(value),
            _ => PhysicalTypeMismatch {
                new: "i64",
                expected: self.type_description(),
            }
            .fail()?,
        }
        Ok(())
    }

    fn push_u64(&mut self, value: u64) -> Result<()> {
        match self {
            ColumnValues::U64(data) => data.push(value),
            _ => PhysicalTypeMismatch {
                new: "u64",
                expected: self.type_description(),
            }
            .fail()?,
        }
        Ok(())
    }

    fn push_f64(&mut self, value: f64) -> Result<()> {
        match self {
            ColumnValues::F64(data) => data.push(value),
            _ => PhysicalTypeMismatch {
                new: "f64",
                expected: self.type_description(),
            }
            .fail()?,
        }
        Ok(())
    }

    fn push_bool(&mut self, value: bool) -> Result<()> {
        match self {
            ColumnValues::Bool(data) => data.push(value),
            ColumnValues::PackedBool(data) => data.push(value),
            _ => PhysicalTypeMismatch {
                new: "bool",
                expected: self.type_description(),
            }
            .fail()?,
        }
        Ok(())
    }

    fn type_description(&self) -> &'static str {
        match &self {
            Self::F64(_) => "f64",
            Self::I64(_) => "i64",
            Self::U64(_) => "u64",
            Self::String(_) => "str",
            Self::PackedString(_) => "packed_str",
            Self::Dictionary(_, _) => "dict",
            Self::PackedBool(_) => "packed_bool",
            Self::Bool(_) => "bool",
        }
    }
}
