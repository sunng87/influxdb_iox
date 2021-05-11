use crate::entry;
use crate::entry_fb::ColumnValues;
use internal_types::write::{ColumnWrite, ColumnWriteValues, TableWrite};

impl<'a> From<entry::TableBatch<'a>> for TableWrite<'a> {
    fn from(batch: entry::TableBatch<'a>) -> Self {
        (&batch).into()
    }
}

impl<'a> From<&entry::TableBatch<'a>> for TableWrite<'a> {
    fn from(batch: &entry::TableBatch<'a>) -> Self {
        Self {
            columns: batch
                .columns()
                .into_iter()
                .map(|batch| {
                    let column = batch.name().into();
                    (column, batch.into())
                })
                .collect(),
        }
    }
}

impl<'a> From<entry::Column<'a>> for ColumnWrite<'a> {
    fn from(batch: entry::Column<'a>) -> Self {
        let values = match batch.inner().values_type() {
            ColumnValues::I64Values => ColumnWriteValues::I64(
                batch
                    .inner()
                    .values_as_i64values()
                    .unwrap()
                    .values()
                    .unwrap()
                    .safe_slice()
                    .into(),
            ),
            ColumnValues::F64Values => ColumnWriteValues::F64(
                batch
                    .inner()
                    .values_as_f64values()
                    .unwrap()
                    .values()
                    .unwrap()
                    .safe_slice()
                    .into(),
            ),
            ColumnValues::U64Values => ColumnWriteValues::U64(
                batch
                    .inner()
                    .values_as_u64values()
                    .unwrap()
                    .values()
                    .unwrap()
                    .safe_slice()
                    .into(),
            ),
            ColumnValues::StringValues => ColumnWriteValues::String(
                // TODO: This is unfortunate, perhaps the FlatBuffer should be packed and/or dictionary encoded
                batch
                    .inner()
                    .values_as_string_values()
                    .unwrap()
                    .values()
                    .unwrap()
                    .iter()
                    .map(Into::into)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ColumnValues::BoolValues => ColumnWriteValues::Bool(
                batch
                    .inner()
                    .values_as_bool_values()
                    .unwrap()
                    .values()
                    .unwrap()
                    .into(),
            ),
            _ => unreachable!(),
        };

        Self {
            row_count: batch.row_count,
            influx_type: batch.influx_type(),
            valid_mask: construct_valid_mask(batch.inner().null_mask(), batch.row_count).into(),
            values,
        }
    }
}

/// Construct a validity mask from the given column's null mask
fn construct_valid_mask(mask: Option<&[u8]>, row_count: usize) -> Vec<u8> {
    let buf_len = (row_count + 7) >> 3;
    match mask {
        Some(data) => {
            debug_assert!(data.len() == buf_len);

            data.iter().map(|x| !x).collect()
        }
        None => {
            // If no null mask they're all valid
            let mut data = Vec::new();
            data.resize(buf_len, 0xFF);
            data
        }
    }
}
