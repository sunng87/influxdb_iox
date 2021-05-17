use crate::arrow::sort::SortOrder;
use arrow::array::{Array, DictionaryArray, Int32Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Int32Type, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::cmp::Ordering;

/// A value that can be used in a primary key
#[derive(Debug, PartialOrd, PartialEq, Ord, Eq)]
enum FieldValue<'a> {
    String(&'a str),
    Time(i64),
}

/// A column that forms part of a primary key
#[derive(Debug)]
enum ColumnValues<'a> {
    Tag(&'a arrow::array::Int32Array, StringArray),
    Time(&'a TimestampNanosecondArray),
}

impl<'a> ColumnValues<'a> {
    fn get(&self, idx: usize) -> Option<FieldValue<'_>> {
        match &self {
            ColumnValues::Tag(keys, values) => {
                if !keys.is_valid(idx) {
                    return None;
                }
                Some(FieldValue::String(values.value(keys.value(idx) as usize)))
            }
            ColumnValues::Time(time) => {
                if !time.is_valid(idx) {
                    return None;
                }
                Some(FieldValue::Time(time.value(idx)))
            }
        }
    }
}

/// A PrimaryKeyCursor is created from a RecordBatch and a list of columns that comprise
/// its primary key. It also contains a list of row indices to advance through
///
/// When created the cursor points to the first row index in the provided indices row.
///
/// Calling PrimaryKeyCursor::advance will move the cursor to point to the next row index
/// identified by the indices, and returns the row index it previously pointed to
///
/// When compared PrimaryKeyCursor's compare the primary keys the cursor currently
/// points to
///
#[derive(Debug)]
struct PrimaryKeyCursor<'a> {
    columns: Vec<Option<ColumnValues<'a>>>,
    // TODO: Make this optional
    indices: &'a [u32],
    row: usize,
}

impl<'a> PrimaryKeyCursor<'a> {
    pub fn new(batch: &'a RecordBatch, indices: &'a [u32], column_names: &[String]) -> Self {
        let schema = batch.schema();
        let columns = column_names
            .iter()
            .map(|column_name| {
                let (idx, field) = schema.column_with_name(column_name)?;
                let column = batch.column(idx);
                Some(match field.data_type() {
                    DataType::Dictionary(key, value)
                        if key.as_ref() == &DataType::Int32
                            && value.as_ref() == &DataType::Utf8 =>
                    {
                        let dictionary = column
                            .as_any()
                            .downcast_ref::<DictionaryArray<Int32Type>>()
                            .unwrap();

                        let keys = dictionary
                            .keys()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();

                        // This is a pretty grim workaround for https://github.com/apache/arrow-rs/issues/313
                        let values = dictionary
                            .values()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .data()
                            .clone();
                        let values = StringArray::from(values);

                        ColumnValues::Tag(keys, values)
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                        let times = column
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();

                        ColumnValues::Time(times)
                    }
                    _ => unreachable!(),
                })
            })
            .collect();

        Self {
            row: 0,
            indices,
            columns,
        }
    }

    pub fn remaining(&self) -> usize {
        self.indices.len() - self.row
    }

    pub fn is_finished(&self) -> bool {
        self.remaining() == 0
    }

    pub fn advance(&mut self) -> usize {
        assert!(!self.is_finished());
        let idx = self.indices[self.row];
        self.row += 1;
        idx as usize
    }
}

impl<'a> Eq for PrimaryKeyCursor<'a> {}
impl<'a> PartialEq for PrimaryKeyCursor<'a> {
    fn eq(&self, other: &Self) -> bool {
        assert!(!self.is_finished());
        assert!(!other.is_finished());
        assert_eq!(self.columns.len(), other.columns.len());

        let l_idx = self.indices[self.row] as usize;
        let r_idx = other.indices[other.row] as usize;

        for (l, r) in self.columns.iter().zip(other.columns.iter()) {
            match (l, r) {
                (Some(l), Some(r)) => match (l.get(l_idx), r.get(r_idx)) {
                    (Some(l), Some(r)) => {
                        if l.ne(&r) {
                            return false;
                        }
                    }
                    _ => return false,
                },
                _ => return false,
            }
        }
        true
    }
}

impl<'a> PartialOrd for PrimaryKeyCursor<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for PrimaryKeyCursor<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        assert!(!self.is_finished());
        assert!(!other.is_finished());
        assert_eq!(self.columns.len(), other.columns.len());

        let l_idx = self.indices[self.row] as usize;
        let r_idx = other.indices[other.row] as usize;

        for (l, r) in self.columns.iter().zip(other.columns.iter()) {
            let l = l.as_ref().and_then(|x| x.get(l_idx));
            let r = r.as_ref().and_then(|x| x.get(r_idx));

            match (l, r) {
                // Sort order is ascending with nulls first
                (None, Some(_)) => return Ordering::Less,
                (Some(_), None) => return Ordering::Greater,
                (None, None) => {}
                (Some(l), Some(r)) => match l.cmp(&r) {
                    Ordering::Equal => {}
                    o => return o,
                },
            }
        }
        Ordering::Equal
    }
}

/// A BatchIndex identifies a row within an ordered collection of RecordBatches
#[derive(Debug, Clone, PartialOrd, PartialEq, Ord, Eq)]
pub struct BatchIndex {
    pub batch_idx: usize,
    pub row_idx: usize,
}

/// A `SortedBatchIterator` is created from a collection of RecordBatches
/// with corresponding indices, and a SortOrder
///
/// The `indices` array for each batch must yield an iteration that corresponds to
/// the provided `SortOrder` within that batch
///
/// The `SortedBatchIterator` will then yield the sequence of `BatchIndex` that correctly
/// interleaves the source rows with respect to the provided SortOrder
#[derive(Debug)]
pub struct SortedBatchIterator<'a> {
    cursors: Vec<PrimaryKeyCursor<'a>>,
}

impl<'a> SortedBatchIterator<'a> {
    pub fn new(
        batch: impl IntoIterator<Item = &'a RecordBatch>,
        indices: impl IntoIterator<Item = &'a [u32]>,
        sort_order: &'a SortOrder,
    ) -> Self {
        let cursors = batch
            .into_iter()
            .zip(indices.into_iter())
            .map(|(batch, sort_indices)| {
                PrimaryKeyCursor::new(batch, sort_indices, sort_order.primary_key.as_slice())
            })
            .collect();
        Self { cursors }
    }
}

impl<'a> Iterator for SortedBatchIterator<'a> {
    type Item = BatchIndex;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: Implement de-duplication

        let mut min_cursor = None;
        for (idx, candidate) in self.cursors.iter_mut().enumerate() {
            if candidate.is_finished() {
                continue;
            }
            match min_cursor {
                None => min_cursor = Some((idx, candidate)),
                Some((_, ref min)) => {
                    if min.cmp(&candidate) == Ordering::Greater {
                        min_cursor = Some((idx, candidate))
                    }
                }
            }
        }
        let (batch_idx, min_cursor) = min_cursor?;
        let row_idx = min_cursor.advance();

        Some(BatchIndex { batch_idx, row_idx })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let total = self.cursors.iter().map(|x| x.remaining()).sum();
        (total, Some(total))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::sort::{compute_sort_indices, SortOrder};
    use arrow::array::ArrayRef;
    use std::iter::FromIterator;
    use std::sync::Arc;

    #[test]
    fn test_cursor_single() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
            "a", "d", "b", "c", "a",
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 4]));

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let sort = SortOrder {
            primary_key: vec!["b".to_string(), "c".to_string()],
        };

        let sort_indices = compute_sort_indices(&batch, &sort).unwrap();
        let collected: Vec<_> =
            SortedBatchIterator::new(&[batch], std::iter::once(sort_indices.values()), &sort)
                .collect();

        assert_eq!(
            collected,
            &[
                BatchIndex {
                    batch_idx: 0,
                    row_idx: 4
                },
                BatchIndex {
                    batch_idx: 0,
                    row_idx: 0
                },
                BatchIndex {
                    batch_idx: 0,
                    row_idx: 2
                },
                BatchIndex {
                    batch_idx: 0,
                    row_idx: 3
                },
                BatchIndex {
                    batch_idx: 0,
                    row_idx: 1
                },
            ]
        )
    }

    #[test]
    fn test_cursor_two() {
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

        let s1 = compute_sort_indices(&b1, &sort).unwrap();
        let s2 = compute_sort_indices(&b2, &sort).unwrap();

        let collected: Vec<_> = SortedBatchIterator::new(
            &[b1, b2],
            std::array::IntoIter::new([s1.values(), s2.values()]),
            &sort,
        )
        .collect();

        assert_eq!(
            collected,
            &[
                BatchIndex {
                    batch_idx: 0,
                    row_idx: 4
                },
                BatchIndex {
                    batch_idx: 1,
                    row_idx: 4
                },
                BatchIndex {
                    batch_idx: 0,
                    row_idx: 0
                },
                BatchIndex {
                    batch_idx: 1,
                    row_idx: 3
                },
                BatchIndex {
                    batch_idx: 0,
                    row_idx: 2
                },
                BatchIndex {
                    batch_idx: 0,
                    row_idx: 3
                },
                BatchIndex {
                    batch_idx: 1,
                    row_idx: 1
                },
                BatchIndex {
                    batch_idx: 0,
                    row_idx: 1
                },
                BatchIndex {
                    batch_idx: 1,
                    row_idx: 2
                },
                BatchIndex {
                    batch_idx: 1,
                    row_idx: 0
                },
            ]
        )
    }
}
