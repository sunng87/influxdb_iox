use std::borrow::Cow;
use std::hash::Hash;

use chrono::Utc;
use hashbrown::HashMap;
use snafu::{ResultExt, Snafu};

use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};

use crate::schema::TIME_COLUMN_NAME;
use crate::write::builder::ColumnWriteBuilder;
use crate::write::TableWrite;

#[derive(Debug, Snafu)]
pub enum Error<E: std::error::Error + Sized + 'static> {
    #[snafu(display("Parse error at line {}: {}", line_number, source))]
    ParseError { line_number: usize, source: E },

    #[snafu(display("Column error at line {}: {}", line_number, source))]
    ColumnError {
        line_number: usize,
        source: crate::write::builder::Error,
    },
}

#[derive(Debug)]
pub struct Options {
    pub default_time: i64,
    pub tag_dictionary: bool,
    pub tag_packed: bool,
    pub string_dictionary: bool,
    pub string_packed: bool,
    pub bool_packed: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            default_time: Utc::now().timestamp_nanos(),
            tag_dictionary: false,
            tag_packed: false,
            string_dictionary: false,
            string_packed: false,
            bool_packed: false,
        }
    }
}

pub fn lp_to_table_writes<'a>(
    lp: &'a str,
    options: &Options,
) -> Result<HashMap<Cow<'a, str>, TableWrite<'a>>, Error<influxdb_line_protocol::Error>> {
    let collected = parse_lines(lp)
        .enumerate()
        .map(|(idx, res)| {
            let line = res.context(ParseError {
                line_number: idx + 1,
            })?;
            Ok(line)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut partitioned = lines_to_table_writes(
        collected
            .iter()
            .map(|line| Ok::<_, influxdb_line_protocol::Error>(((), line))),
        options,
    )?;

    Ok(partitioned.remove(&()).unwrap())
}

pub fn lines_to_table_writes<'a, 'b, K, I, E>(
    lines: I,
    options: &Options,
) -> Result<HashMap<K, HashMap<Cow<'a, str>, TableWrite<'a>>>, Error<E>>
where
    'a: 'b,
    K: 'a + PartialEq + Eq + Hash,
    I: Iterator<Item = Result<(K, &'b ParsedLine<'a>), E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut partitioned_builders: HashMap<
        K,
        HashMap<Cow<'a, str>, (usize, HashMap<Cow<'a, str>, ColumnWriteBuilder<'a>>)>,
    > = Default::default();

    for (idx, line) in lines.enumerate() {
        let (key, line) = line.context(ParseError {
            line_number: idx + 1,
        })?;

        let builders = partitioned_builders.entry(key).or_default();
        let (rows, table) = builders
            .entry((&line.series.measurement).into())
            .or_default();
        let start_rows = *rows;
        *rows += 1;

        if let Some(tagset) = &line.series.tag_set {
            for (key, value) in tagset {
                let builder = table.entry(key.into()).or_insert_with(|| {
                    ColumnWriteBuilder::new_tag_column(
                        options.tag_dictionary,
                        options.tag_packed,
                        start_rows,
                    )
                });
                builder.push_tag(value.into()).context(ColumnError {
                    line_number: idx + 1,
                })?
            }
        }

        for (key, value) in &line.field_set {
            match value {
                FieldValue::I64(data) => {
                    let builder = table
                        .entry(key.into())
                        .or_insert_with(|| ColumnWriteBuilder::new_i64_column(start_rows));
                    builder.push_i64(*data).context(ColumnError {
                        line_number: idx + 1,
                    })?;
                }
                FieldValue::U64(data) => {
                    let builder = table
                        .entry(key.into())
                        .or_insert_with(|| ColumnWriteBuilder::new_u64_column(start_rows));
                    builder.push_u64(*data).context(ColumnError {
                        line_number: idx + 1,
                    })?;
                }
                FieldValue::F64(data) => {
                    let builder = table
                        .entry(key.into())
                        .or_insert_with(|| ColumnWriteBuilder::new_f64_column(start_rows));
                    builder.push_f64(*data).context(ColumnError {
                        line_number: idx + 1,
                    })?;
                }
                FieldValue::String(data) => {
                    let builder = table.entry(key.into()).or_insert_with(|| {
                        ColumnWriteBuilder::new_string_column(
                            options.string_dictionary,
                            options.string_packed,
                            start_rows,
                        )
                    });
                    builder.push_string(data.into()).context(ColumnError {
                        line_number: idx + 1,
                    })?;
                }
                FieldValue::Boolean(data) => {
                    let builder = table.entry(key.into()).or_insert_with(|| {
                        ColumnWriteBuilder::new_bool_column(options.bool_packed, start_rows)
                    });
                    builder.push_bool(*data).context(ColumnError {
                        line_number: idx + 1,
                    })?;
                }
            }
        }

        let builder = table
            .entry(TIME_COLUMN_NAME.into())
            .or_insert_with(|| ColumnWriteBuilder::new_time_column(start_rows));

        builder
            .push_time(line.timestamp.unwrap_or_else(|| options.default_time))
            .unwrap();

        for builder in table.values_mut() {
            builder.null_to_idx(*rows)
        }
    }

    Ok(partitioned_builders
        .into_iter()
        .map(|(k, builders)| {
            (
                k,
                builders
                    .into_iter()
                    .map(|(name, (_, columns))| {
                        (
                            name,
                            TableWrite {
                                columns: columns
                                    .into_iter()
                                    .map(|(column_name, builder)| (column_name, builder.build()))
                                    .collect(),
                            },
                        )
                    })
                    .collect(),
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use crate::schema::{InfluxColumnType, InfluxFieldType};

    use super::*;

    #[test]
    fn test_basic() {
        let lp = r#"
            a,host=a ival=23i,fval=1.2,uval=7u,sval="hi",bval=true 1
            a,host=b ival=22i,fval=2.2,uval=1u,sval="world",bval=false 2
        "#;

        let writes = lp_to_table_writes(lp, &Options::default()).unwrap();

        assert_eq!(writes.len(), 1);
        assert_eq!(writes["a"].columns.len(), 7);

        let columns = &writes["a"].columns;

        assert_eq!(columns["host"].influx_type, InfluxColumnType::Tag);
        assert_eq!(columns["host"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["host"].values.string().unwrap(), &["a", "b"]);

        assert_eq!(
            columns["ival"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::Integer)
        );
        assert_eq!(columns["ival"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["ival"].values.i64().unwrap(), &[23, 22]);

        assert_eq!(
            columns["fval"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::Float)
        );
        assert_eq!(columns["fval"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["fval"].values.f64().unwrap(), &[1.2, 2.2]);

        assert_eq!(
            columns["uval"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::UInteger)
        );
        assert_eq!(columns["uval"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["uval"].values.u64().unwrap(), &[7, 1]);

        assert_eq!(
            columns["sval"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        assert_eq!(columns["sval"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["sval"].values.string().unwrap(), &["hi", "world"]);

        assert_eq!(
            columns["bval"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::Boolean)
        );
        assert_eq!(columns["bval"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["bval"].values.bool().unwrap(), &[true, false]);

        assert_eq!(
            columns[TIME_COLUMN_NAME].influx_type,
            InfluxColumnType::Timestamp
        );
        assert_eq!(columns[TIME_COLUMN_NAME].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns[TIME_COLUMN_NAME].values.i64().unwrap(), &[1, 2]);
    }

    #[test]
    fn test_nulls() {
        let lp = r#"
            a,host=a val=23i 983
            a val=21i,bool=true,string="hello" 222
            a,host=a,region=west val2=23.2
        "#;

        let options = Options {
            default_time: 32,
            ..Default::default()
        };

        let writes = lp_to_table_writes(lp, &options).unwrap();

        assert_eq!(writes.len(), 1);
        assert_eq!(writes["a"].columns.len(), 7);

        let columns = &writes["a"].columns;

        assert_eq!(
            columns[TIME_COLUMN_NAME].influx_type,
            InfluxColumnType::Timestamp
        );
        assert_eq!(columns[TIME_COLUMN_NAME].valid_mask.as_ref(), &[0b00000111]);
        assert_eq!(
            columns[TIME_COLUMN_NAME].values.i64().unwrap(),
            &[983, 222, options.default_time]
        );

        assert_eq!(columns["host"].influx_type, InfluxColumnType::Tag);
        assert_eq!(columns["host"].valid_mask.as_ref(), &[0b00000101]);
        assert_eq!(columns["host"].values.string().unwrap(), &["a", "a"]);

        assert_eq!(
            columns["val"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::Integer)
        );
        assert_eq!(columns["val"].valid_mask.as_ref(), &[0b00000011]);
        assert_eq!(columns["val"].values.i64().unwrap(), &[23, 21]);
    }

    #[test]
    fn test_multiple_table() {
        let lp = r#"
            cpu val=1 55
            mem val=23 10
            cpu val=88 100
            disk foo=23.2 110
            mem val=55 111
        "#;
        let writes = lp_to_table_writes(lp, &Options::default()).unwrap();
        assert_eq!(writes.len(), 3);
        assert!(writes.contains_key("cpu"));
        assert!(writes.contains_key("mem"));
        assert!(writes.contains_key("disk"));
    }

    #[test]
    fn test_packed_strings() {
        let lp = r#"
            a,foo=bar val="cupcakes" 1
            a,foo=bar val="bongo" 2
            a,foo=banana val="cupcakes" 3
            a,foo=bar val="cupcakes" 4
            a,foo=bar val="bongo" 5
        "#;
        let options = Options {
            string_packed: true,
            tag_dictionary: true,
            ..Default::default()
        };

        let writes = lp_to_table_writes(lp, &options).unwrap();
        assert_eq!(writes.len(), 1);
        let columns = &writes["a"].columns;

        assert_eq!(
            columns["val"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        let val = columns["val"].values.packed_string().unwrap();
        assert_eq!(val.values.as_ref(), "cupcakesbongocupcakescupcakesbongo");
        assert_eq!(val.indexes.as_ref(), &[0, 8, 13, 21, 29, 34]);

        assert_eq!(columns["foo"].influx_type, InfluxColumnType::Tag);
        let foo = columns["foo"].values.dictionary().unwrap();
        assert_eq!(foo.keys.as_ref(), &[0, 0, 1, 0, 0]);
        assert_eq!(foo.values.values.as_ref(), "barbanana");
        assert_eq!(foo.values.indexes.as_ref(), &[0, 3, 9]);
    }

    #[test]
    fn test_packed_bool() {
        let lp = r#"
            a,foo=bar val=true 1
            a,foo=bar val=true 2
            a,foo=banana val=false 3
            a,foo=bar val=true 4
            a,foo=bar val=false 5
        "#;
        let options = Options {
            tag_packed: true,
            bool_packed: true,
            ..Default::default()
        };

        let writes = lp_to_table_writes(lp, &options).unwrap();
        assert_eq!(writes.len(), 1);
        let columns = &writes["a"].columns;

        assert_eq!(
            columns["val"].influx_type,
            InfluxColumnType::Field(InfluxFieldType::Boolean)
        );
        assert_eq!(columns["val"].values.packed_bool().unwrap(), &[0b00001011]);

        assert_eq!(columns["foo"].influx_type, InfluxColumnType::Tag);
        let foo = columns["foo"].values.packed_string().unwrap();
        assert_eq!(foo.values.as_ref(), "barbarbananabarbar");
        assert_eq!(foo.indexes.as_ref(), &[0, 3, 6, 12, 15, 18]);
    }

    #[test]
    fn test_partition() {
        let lp = r#"
            a,foo=bar val="cupcakes" 1
            a,foo=bar val="bongo" 2
            a,foo=banana val="cupcakes" 3
            a,foo=bar val="cupcakes" 4
            a,foo=bar val="bongo" 5
            b,foo=bar val="bongo" 6
            a,foo=bar val="bongo" 7
            a,foo=bar val="bongo" 8
            a,foo=banana val="cupcakes" 9
            a,foo=bar val="cupcakes" 10
            a,foo=banana val="cupcakes" 11
        "#;
        let options = Options {
            string_packed: true,
            tag_dictionary: true,
            ..Default::default()
        };

        let lines = parse_lines(lp).collect::<Result<Vec<_>, _>>().unwrap();

        let writes = lines_to_table_writes::<Cow<'_, str>, _, _>(
            lines.iter().map(|line| {
                Ok::<_, std::convert::Infallible>((
                    line.series.tag_set.as_ref().unwrap()[0].1.clone().into(),
                    line,
                ))
            }),
            &options,
        )
        .unwrap();

        assert_eq!(writes.len(), 2);
        assert_eq!(writes["bar"].len(), 2);
        assert_eq!(writes["banana"].len(), 1);

        assert_eq!(writes["bar"]["a"].columns.len(), 3);
        assert_eq!(writes["bar"]["b"].columns.len(), 3);
        assert_eq!(writes["banana"]["a"].columns.len(), 3);

        assert_eq!(writes["bar"]["a"].columns["val"].row_count, 7);
        assert_eq!(writes["bar"]["b"].columns["val"].row_count, 1);
        assert_eq!(writes["banana"]["a"].columns["val"].row_count, 3);

        assert_eq!(
            writes["bar"]["a"].columns[TIME_COLUMN_NAME]
                .values
                .i64()
                .unwrap(),
            &[1, 2, 4, 5, 7, 8, 10]
        );
        assert_eq!(
            writes["bar"]["b"].columns[TIME_COLUMN_NAME]
                .values
                .i64()
                .unwrap(),
            &[6]
        );
        assert_eq!(
            writes["banana"]["a"].columns[TIME_COLUMN_NAME]
                .values
                .i64()
                .unwrap(),
            &[3, 9, 11]
        );
    }
}
