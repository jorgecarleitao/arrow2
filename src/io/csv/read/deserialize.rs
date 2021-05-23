use std::sync::Arc;

use chrono::Datelike;
use csv::ByteRecord;

use crate::{
    array::*,
    datatypes::*,
    error::{ArrowError, Result},
    record_batch::RecordBatch,
    temporal_conversions::EPOCH_DAYS_FROM_CE,
    types::NativeType,
};

fn deserialize_primitive<T, F>(
    rows: &[ByteRecord],
    column: usize,
    datatype: DataType,
    op: F,
) -> Arc<dyn Array>
where
    T: NativeType + lexical_core::FromLexical,
    F: Fn(&[u8]) -> Option<T>,
{
    let iter = rows.iter().map(|row| match row.get(column) {
        Some(bytes) => {
            if bytes.is_empty() {
                return None;
            }
            op(bytes)
        }
        None => None,
    });
    Arc::new(Primitive::<T>::from_trusted_len_iter(iter).to(datatype))
}

fn deserialize_boolean<F>(rows: &[ByteRecord], column: usize, op: F) -> Arc<dyn Array>
where
    F: Fn(&[u8]) -> Option<bool>,
{
    let iter = rows.iter().map(|row| match row.get(column) {
        Some(bytes) => {
            if bytes.is_empty() {
                return None;
            }
            op(bytes)
        }
        None => None,
    });
    Arc::new(BooleanArray::from_trusted_len_iter(iter))
}

fn deserialize_utf8<O: Offset>(rows: &[ByteRecord], column: usize) -> Arc<dyn Array> {
    let iter = rows.iter().map(|row| match row.get(column) {
        Some(bytes) => std::str::from_utf8(bytes).ok(),
        None => None,
    });
    Arc::new(Utf8Array::<O>::from_trusted_len_iter(iter))
}

pub fn deserialize_column(
    rows: &[ByteRecord],
    column: usize,
    datatype: DataType,
    _line_number: usize,
) -> Result<Arc<dyn Array>> {
    use DataType::*;
    Ok(match datatype {
        Boolean => deserialize_boolean(rows, column, |bytes| {
            if bytes.eq_ignore_ascii_case(b"false") {
                Some(false)
            } else if bytes.eq_ignore_ascii_case(b"true") {
                Some(true)
            } else {
                None
            }
        }),
        Int8 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<i8>(bytes).ok()
        }),
        Int16 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<i16>(bytes).ok()
        }),
        Int32 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<i32>(bytes).ok()
        }),
        Int64 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<i64>(bytes).ok()
        }),
        UInt8 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<u8>(bytes).ok()
        }),
        UInt16 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<u16>(bytes).ok()
        }),
        UInt32 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<u32>(bytes).ok()
        }),
        UInt64 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<u64>(bytes).ok()
        }),
        Float32 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<f32>(bytes).ok()
        }),
        Float64 => deserialize_primitive(rows, column, datatype, |bytes| {
            lexical_core::parse::<f64>(bytes).ok()
        }),
        Date32 => deserialize_primitive(rows, column, datatype, |bytes| {
            std::str::from_utf8(bytes)
                .ok()
                .and_then(|x| x.parse::<chrono::NaiveDate>().ok())
                .map(|x| x.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
        }),
        Date64 => deserialize_primitive(rows, column, datatype, |bytes| {
            std::str::from_utf8(bytes)
                .ok()
                .and_then(|x| x.parse::<chrono::NaiveDateTime>().ok())
                .map(|x| x.timestamp_millis())
        }),
        Timestamp(TimeUnit::Nanosecond, None) => {
            deserialize_primitive(rows, column, datatype, |bytes| {
                std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|x| x.parse::<chrono::NaiveDateTime>().ok())
                    .map(|x| x.timestamp_nanos())
            })
        }
        Timestamp(TimeUnit::Microsecond, None) => {
            deserialize_primitive(rows, column, datatype, |bytes| {
                std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|x| x.parse::<chrono::NaiveDateTime>().ok())
                    .map(|x| x.timestamp_nanos() / 1000)
            })
        }
        Timestamp(TimeUnit::Millisecond, None) => {
            deserialize_primitive(rows, column, datatype, |bytes| {
                std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|x| x.parse::<chrono::NaiveDateTime>().ok())
                    .map(|x| x.timestamp_nanos() / 1_000_000)
            })
        }
        Timestamp(TimeUnit::Second, None) => {
            deserialize_primitive(rows, column, datatype, |bytes| {
                std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|x| x.parse::<chrono::NaiveDateTime>().ok())
                    .map(|x| x.timestamp_nanos() / 1_000_000_000)
            })
        }
        Utf8 => deserialize_utf8::<i32>(rows, column),
        LargeUtf8 => deserialize_utf8::<i64>(rows, column),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Deserializing type \"{:?}\" is not implemented",
                other
            )))
        }
    })
}

/// Deserializes rows [`ByteRecord`] into a [`RecordBatch`].
/// Note that this is a convenience function: column deserialization
///is trivially parallelizable (e.g. rayon).
pub fn deserialize_batch<F>(
    rows: &[ByteRecord],
    fields: &[Field],
    projection: Option<&[usize]>,
    line_number: usize,
    deserialize_column: F,
) -> Result<RecordBatch>
where
    F: Fn(&[ByteRecord], usize, DataType, usize) -> Result<Arc<dyn Array>>,
{
    let projection: Vec<usize> = match projection {
        Some(ref v) => v.to_vec(),
        None => fields.iter().enumerate().map(|(i, _)| i).collect(),
    };
    let projected_fields: Vec<Field> = projection.iter().map(|i| fields[*i].clone()).collect();

    let schema = Arc::new(Schema::new(projected_fields));

    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let columns = projection
        .iter()
        .map(|column| {
            let column = *column;
            let field = &fields[column];
            let data_type = field.data_type();
            deserialize_column(rows, column, data_type.clone(), line_number)
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema, columns)
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;

    fn test(input: &str, data_type: DataType) -> Result<Arc<dyn Array>> {
        let reader = std::io::Cursor::new(input);
        let mut reader = ReaderBuilder::new().has_headers(false).from_reader(reader);
        let rows = read_rows(&mut reader, 0, 10)?;
        deserialize_column(&rows, 0, data_type, 0)
    }

    #[test]
    fn int32() -> Result<()> {
        let result = test("1,\n,\n3,", DataType::Int32)?;
        let expected = Int32Array::from(&[Some(1), None, Some(3)]);
        assert_eq!(expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn date32() -> Result<()> {
        let result = test("1970-01-01,\n2020-03-15,\n1945-05-08,\n", DataType::Date32)?;
        let expected =
            Primitive::<i32>::from(&[Some(0), Some(18336), Some(-9004)]).to(DataType::Date32);
        assert_eq!(expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn date64() -> Result<()> {
        let input = "1970-01-01T00:00:00,\n \
            2018-11-13T17:11:10,\n \
            2018-11-13T17:11:10.011,\n \
            1900-02-28T12:34:56,\n";

        let result = test(input, DataType::Date64)?;
        let expected = Primitive::<i64>::from(&[
            Some(0),
            Some(1542129070000),
            Some(1542129070011),
            Some(-2203932304000),
        ])
        .to(DataType::Date64);
        assert_eq!(expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn boolean() -> Result<()> {
        let input = vec!["true", "True", "False", "F", "t"];
        let input = input.join("\n");

        let expected = BooleanArray::from(&[Some(true), Some(true), Some(false), None, None]);

        let result = test(&input, DataType::Boolean)?;

        assert_eq!(expected, result.as_ref());
        Ok(())
    }

    #[test]
    fn float32() -> Result<()> {
        let input = vec!["12.34", "12", "0.0", "inf", "-inf", "dd"];
        let input = input.join("\n");

        let expected = Float32Array::from(&[
            Some(12.34),
            Some(12.0),
            Some(0.0),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            None,
        ]);

        let result = test(&input, DataType::Float32)?;
        assert_eq!(expected, result.as_ref());
        Ok(())
    }
}
