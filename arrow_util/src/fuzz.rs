//! A collection of testing functions for arrow based code
use arrow::{
    array::{
        Array, ArrayDataBuilder, ArrayRef, DictionaryArray, Float64Array, Int32Array, StringArray,
        TimestampNanosecondArray, UInt64Array,
    },
    datatypes::{DataType, Int32Type, TimeUnit},
};
use rand::Rng;
use std::sync::Arc;

pub fn make_random_array(
    datatype: DataType,
    dictionary_values: Option<usize>,
    num_rows: usize,
    valid_probability: f64,
) -> ArrayRef {
    let mut rng = rand::thread_rng();

    match dictionary_values {
        Some(dictionary_values) => {
            let values = make_random_array(datatype.clone(), None, dictionary_values, 1.);
            let indexes: Int32Array = std::iter::from_fn(|| {
                Some(
                    rng.gen_bool(valid_probability)
                        .then(|| rng.gen_range(0..(dictionary_values as i32))),
                )
            })
            .take(num_rows)
            .collect();

            let data = ArrayDataBuilder::new(DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(datatype),
            ))
            .len(indexes.len())
            .add_buffer(indexes.data().buffers()[0].clone())
            .null_bit_buffer(indexes.data().null_buffer().unwrap().clone())
            .add_child_data(values.data().clone())
            .build();

            Arc::new(DictionaryArray::<Int32Type>::from(data))
        }
        None => match datatype {
            DataType::UInt64 => {
                let array: UInt64Array =
                    std::iter::from_fn(|| Some(rng.gen_bool(valid_probability).then(|| rng.gen())))
                        .take(num_rows)
                        .collect();

                Arc::new(array)
            }
            DataType::Float64 => {
                let array: Float64Array =
                    std::iter::from_fn(|| Some(rng.gen_bool(valid_probability).then(|| rng.gen())))
                        .take(num_rows)
                        .collect();

                Arc::new(array)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                let array: TimestampNanosecondArray = std::iter::from_fn(|| {
                    Some(
                        rng.gen_bool(valid_probability)
                            .then(|| rng.gen_range(0..500000)),
                    )
                })
                .take(num_rows)
                .collect();

                Arc::new(array)
            }
            DataType::Utf8 => {
                let array: StringArray = std::iter::from_fn(|| {
                    Some(
                        rng.gen_bool(valid_probability)
                            .then(|| rng.gen::<usize>().to_string()),
                    )
                })
                .take(num_rows)
                .collect();

                Arc::new(array)
            }
            _ => unimplemented!(),
        },
    }
}
