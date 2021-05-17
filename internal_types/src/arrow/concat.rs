use crate::schema;
use crate::schema::Schema;
use arrow::record_batch::RecordBatch;
use snafu::Snafu;
use std::convert::TryFrom;
use std::sync::Arc;

/// Database schema creation / validation errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    SchemaError { source: schema::Error },

    #[snafu(context(false))]
    ArrowError { source: arrow::error::ArrowError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Concatenates multiple batches together
pub fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = compute_schema(batches).unwrap();

    let columns = schema
        .iter()
        .map(|(_, field)| {
            let arrays: Vec<_> = batches
                .iter()
                .map(|x| match x.schema().column_with_name(field.name()) {
                    Some((idx, _)) => Arc::clone(x.column(idx)),
                    None => arrow::array::new_null_array(field.data_type(), x.num_rows()),
                })
                .collect();

            // This is nasty
            let array_refs: Vec<_> = arrays.iter().map(|x| x.as_ref()).collect();

            arrow::compute::concat(array_refs.as_slice())
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema.as_arrow(), columns)
}

/// Computes the aggregate schema across all provided record batches
pub fn compute_schema(batches: &[RecordBatch]) -> Result<Schema> {
    let mut aggregated_schema = Schema::try_from(batches[0].schema())?;

    for batch in &batches[1..] {
        let schema = Schema::try_from(batch.schema())?;
        aggregated_schema = aggregated_schema.try_merge(schema)?;
    }

    Ok(aggregated_schema)
}
