use snafu::Snafu;

use arrow::array::MutableArrayData;
use arrow::record_batch::RecordBatch;

use crate::arrow::cursor::SortedBatchIterator;
use crate::arrow::sort::{compute_sort_indices, SortOrder};
use crate::arrow::{concat, sort};
use crate::schema;
use std::cmp::min;

/// Database schema creation / validation errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    SchemaError { source: schema::Error },

    #[snafu(context(false))]
    SortError { source: sort::Error },

    #[snafu(context(false))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(context(false))]
    ConcatError { source: concat::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Merges a collection of RecordBatches according to a given sort order producing a list
/// of batches of at most `batch_size` rows
pub fn merge(
    batches: &[RecordBatch],
    batch_size: usize,
    sort_order: &SortOrder,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    // This also validates that the batches are compatible
    let schema = concat::compute_schema(batches)?;

    let total_rows: usize = batches.iter().map(|x| x.num_rows()).sum();

    // TODO: Avoid sorting data if it is already sorted
    let sort_indices = batches
        .iter()
        .map(|x| compute_sort_indices(x, &sort_order))
        .collect::<Result<Vec<_>, _>>()?;

    let mut batch_indexes = SortedBatchIterator::new(
        batches,
        sort_indices.iter().map(|x| x.values()),
        &sort_order,
    );

    let mut ret = vec![];
    let mut has_more = true;

    while has_more {
        // Creates a MutableArrayData for each column in schema
        // Because not all record batches contain all columns also contains a lookup
        // table from batch index to index within the MutableArrayData's buffers
        let mut column_builders: Vec<_> = schema
            .iter()
            .map(|(_, field)| {
                let mut builder_offset = 0_usize;
                let mut builder_offsets = vec![None; batches.len()];

                let arrays: Vec<_> = batches
                    .iter()
                    .enumerate()
                    .flat_map(|(batch_idx, batch)| {
                        let (idx, _) = batch.schema().column_with_name(field.name())?;

                        builder_offsets[batch_idx] = Some(builder_offset);
                        builder_offset += 1;

                        Some(batch.column(idx).data())
                    })
                    .collect();

                assert_eq!(builder_offset, arrays.len());

                (
                    MutableArrayData::new(arrays, true, min(batch_size, total_rows)),
                    builder_offsets,
                )
            })
            .collect();

        // TODO: Could coalesce multiple consecutive reads from the same batch into a single extend

        for _ in 0..batch_size {
            match batch_indexes.next() {
                Some(batch_idx) => {
                    for (builder, lookup) in &mut column_builders {
                        match lookup[batch_idx.batch_idx] {
                            Some(builder_idx) => builder.extend(
                                builder_idx,
                                batch_idx.row_idx,
                                batch_idx.row_idx + 1,
                            ),
                            None => builder.extend_nulls(1),
                        }
                    }
                }
                None => {
                    has_more = false;
                    break;
                }
            }
        }

        let arrays: Vec<_> = column_builders
            .into_iter()
            .map(|(builder, _)| arrow::array::make_array(builder.freeze()))
            .collect();

        ret.push(RecordBatch::try_new(schema.as_arrow(), arrays)?);
    }

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::sort::sort_record_batch_by;
    use crate::schema::TIME_DATA_TYPE;
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Int32Type};
    use arrow_util::{assert_batches_eq, fuzz::make_random_array};
    use std::iter::FromIterator;
    use std::sync::Arc;

    #[test]
    fn test_merge() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
            "a", "d", "b", "c", "a",
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 4]));

        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let b: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
            "f", "c", "e", "b", "a",
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));

        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let sort = SortOrder {
            primary_key: vec!["b".to_string(), "c".to_string()],
        };

        let merged = merge(&[b1, b2], usize::MAX, &sort).unwrap();
        assert_eq!(merged.len(), 1);
        let merged = merged.into_iter().next().unwrap();

        assert_batches_eq!(
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 3 | a | 1970-01-01 00:00:00.000000004 |",
                "| 5 | a | 1970-01-01 00:00:00.000000006 |",
                "| 1 | a | 1970-01-01 00:00:00.000000008 |",
                "| 4 | b | 1970-01-01 00:00:00.000000002 |",
                "| 7 | b | 1970-01-01 00:00:00.000000006 |",
                "| 9 | c | 1970-01-01 00:00:00.000000005 |",
                "| 2 | c | 1970-01-01 00:00:00.000000006 |",
                "| 2 | d | 1970-01-01 00:00:00.000000007 |",
                "| 3 | e | 1970-01-01 00:00:00.000000002 |",
                "| 1 | f | 1970-01-01 00:00:00.000000004 |",
                "+---+---+-------------------------------+",
            ],
            &[merged]
        );
    }

    #[test]
    fn test_merge_nulls() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
            Some("a"),
            Some("d"),
            Some("b"),
            None,
            Some("c"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 4]));

        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let b: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
            None,
            Some("c"),
            Some("e"),
            Some("b"),
            Some("a"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));

        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let sort = SortOrder {
            primary_key: vec!["b".to_string(), "c".to_string()],
        };

        let merged = merge(&[b1, b2], usize::MAX, &sort).unwrap();
        assert_eq!(merged.len(), 1);
        let merged = merged.into_iter().next().unwrap();

        assert_batches_eq!(
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 |   | 1970-01-01 00:00:00.000000004 |",
                "| 9 |   | 1970-01-01 00:00:00.000000005 |",
                "| 5 | a | 1970-01-01 00:00:00.000000006 |",
                "| 1 | a | 1970-01-01 00:00:00.000000008 |",
                "| 4 | b | 1970-01-01 00:00:00.000000002 |",
                "| 7 | b | 1970-01-01 00:00:00.000000006 |",
                "| 3 | c | 1970-01-01 00:00:00.000000004 |",
                "| 2 | c | 1970-01-01 00:00:00.000000006 |",
                "| 2 | d | 1970-01-01 00:00:00.000000007 |",
                "| 3 | e | 1970-01-01 00:00:00.000000002 |",
                "+---+---+-------------------------------+",
            ],
            &[merged]
        );
    }

    #[test]
    fn test_fuzz() {
        let const_array = |val: i32, len: usize| {
            Arc::new(
                std::iter::repeat(Some(val))
                    .take(len)
                    .collect::<Int32Array>(),
            )
        };

        let index_array =
            |len: i32| Arc::new((0..len).into_iter().map(Some).collect::<Int32Array>());

        let batches = &[
            RecordBatch::try_from_iter_with_nullable(vec![
                (
                    "a_u64",
                    make_random_array(DataType::UInt64, None, 10, 0.7),
                    true,
                ),
                (
                    "a_f64",
                    make_random_array(DataType::Float64, None, 10, 0.3),
                    true,
                ),
                (
                    "a",
                    make_random_array(DataType::Utf8, Some(32), 10, 1.),
                    true,
                ),
                (
                    "b",
                    make_random_array(DataType::Utf8, Some(5), 10, 0.5),
                    true,
                ),
                (
                    "c",
                    make_random_array(DataType::Utf8, Some(10), 10, 0.9),
                    true,
                ),
                (
                    "time",
                    make_random_array(TIME_DATA_TYPE(), None, 10, 1.0),
                    false,
                ),
                ("batch", const_array(0, 10), false),
                ("idx", index_array(10), false),
            ])
            .unwrap(),
            RecordBatch::try_from_iter_with_nullable(vec![
                (
                    "a_u64",
                    make_random_array(DataType::UInt64, None, 100, 0.7),
                    true,
                ),
                (
                    "a_f64",
                    make_random_array(DataType::Float64, None, 100, 0.3),
                    true,
                ),
                (
                    "b",
                    make_random_array(DataType::Utf8, Some(5), 100, 0.5),
                    true,
                ),
                (
                    "c",
                    make_random_array(DataType::Utf8, Some(10), 100, 0.9),
                    true,
                ),
                (
                    "d",
                    make_random_array(DataType::Utf8, Some(10), 100, 0.9),
                    true,
                ),
                (
                    "time",
                    make_random_array(TIME_DATA_TYPE(), None, 100, 1.0),
                    false,
                ),
                ("batch", const_array(1, 100), false),
                ("idx", index_array(100), false),
            ])
            .unwrap(),
            RecordBatch::try_from_iter_with_nullable(vec![
                (
                    "a_u64",
                    make_random_array(DataType::UInt64, None, 50, 0.7),
                    true,
                ),
                (
                    "a_f64",
                    make_random_array(DataType::Float64, None, 50, 0.3),
                    true,
                ),
                (
                    "a",
                    make_random_array(DataType::Utf8, Some(12), 50, 0.5),
                    true,
                ),
                (
                    "c",
                    make_random_array(DataType::Utf8, Some(5), 50, 0.9),
                    true,
                ),
                (
                    "d",
                    make_random_array(DataType::Utf8, Some(7), 50, 0.9),
                    true,
                ),
                (
                    "time",
                    make_random_array(TIME_DATA_TYPE(), None, 50, 1.0),
                    false,
                ),
                ("batch", const_array(2, 50), false),
                ("idx", index_array(50), false),
            ])
            .unwrap(),
            RecordBatch::try_from_iter_with_nullable(vec![
                (
                    "a_u64",
                    make_random_array(DataType::UInt64, None, 9, 0.7),
                    true,
                ),
                (
                    "a_f64",
                    make_random_array(DataType::Float64, None, 9, 0.3),
                    true,
                ),
                (
                    "d",
                    make_random_array(DataType::Utf8, Some(2), 9, 0.9),
                    true,
                ),
                (
                    "time",
                    make_random_array(TIME_DATA_TYPE(), None, 9, 1.0),
                    false,
                ),
                ("batch", const_array(3, 9), false),
                ("idx", index_array(9), false),
            ])
            .unwrap(),
        ];

        let sort_order = SortOrder {
            primary_key: vec!["b", "c", "d", "a", "time"]
                .into_iter()
                .map(ToString::to_string)
                .collect(),
        };

        let merged = merge(batches, usize::MAX, &sort_order).unwrap();
        assert_eq!(merged.len(), 1);
        let merged = merged.into_iter().next().unwrap();

        let merge_limited = merge(batches, 100, &sort_order).unwrap();

        let concat = concat::concat_batches(batches).unwrap();
        let sorted = sort_record_batch_by(concat, &sort_order).unwrap();

        assert_eq!((merged.num_rows() + 99) / 100, merge_limited.len());

        for batch in &merge_limited[0..(merge_limited.len() - 1)] {
            assert_eq!(batch.num_rows(), 100)
        }

        let merged = arrow::util::pretty::pretty_format_batches(&[merged]).unwrap();
        let merged = merged.trim().split('\n').collect::<Vec<_>>();

        let limited = arrow::util::pretty::pretty_format_batches(&merge_limited).unwrap();
        let limited = limited.trim().split('\n').collect::<Vec<_>>();

        let sorted = arrow::util::pretty::pretty_format_batches(&[sorted]).unwrap();
        let sorted = sorted.trim().split('\n').collect::<Vec<_>>();

        assert_eq!(
            sorted, merged,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            sorted, merged
        );

        assert_eq!(
            sorted, limited,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            sorted, limited
        )
    }
}
