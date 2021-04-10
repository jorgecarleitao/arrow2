mod boolean;
mod primitive;
mod utf8;
mod utils;

use crate::{
    array::Array,
    datatypes::{DataType, TimeUnit},
    error::{ArrowError, Result},
};

use parquet2::{
    error::ParquetError,
    metadata::ColumnDescriptor,
    read::CompressedPage,
    schema::{
        types::{LogicalType, ParquetType, PhysicalType, PrimitiveConvertedType},
        TimeUnit as ParquetTimeUnit, TimestampType,
    },
};

fn page_iter_to_array_i64<I: Iterator<Item = std::result::Result<CompressedPage, ParquetError>>>(
    iter: I,
    descriptor: &ColumnDescriptor,
    converted_type: &Option<PrimitiveConvertedType>,
    logical_type: &Option<LogicalType>,
) -> Result<Box<dyn Array>> {
    let data_type = match (converted_type, logical_type) {
        (None, None) => DataType::Int64,
        (
            _,
            Some(LogicalType::TIMESTAMP(TimestampType {
                is_adjusted_to_u_t_c,
                unit,
            })),
        ) => {
            let timezone = if *is_adjusted_to_u_t_c {
                // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
                // A TIMESTAMP with isAdjustedToUTC=true is defined as [...] elapsed since the Unix epoch
                Some("+00:00".to_string())
            } else {
                // PARQUET:
                // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
                // A TIMESTAMP with isAdjustedToUTC=false represents [...] such
                // timestamps should always be displayed the same way, regardless of the local time zone in effect
                // ARROW:
                // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
                // If the time zone is null or equal to an empty string, the data is "time
                // zone naive" and shall be displayed *as is* to the user, not localized
                // to the locale of the user.
                None
            };

            match unit {
                ParquetTimeUnit::MILLIS(_) => DataType::Timestamp(TimeUnit::Millisecond, timezone),
                ParquetTimeUnit::MICROS(_) => DataType::Timestamp(TimeUnit::Microsecond, timezone),
                ParquetTimeUnit::NANOS(_) => DataType::Timestamp(TimeUnit::Nanosecond, timezone),
            }
        }
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
        // *Backward compatibility:*
        // TIME_MILLIS | TimeType (isAdjustedToUTC = true, unit = MILLIS)
        // TIME_MICROS | TimeType (isAdjustedToUTC = true, unit = MICROS)
        (Some(PrimitiveConvertedType::TimeMillis), None) => {
            DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".to_string()))
        }
        (Some(PrimitiveConvertedType::TimeMicros), None) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".to_string()))
        }
        (c, l) => {
            return Err(ArrowError::NotYetImplemented(format!(
                "The conversion of (Int64, {:?}, {:?}) to arrow still not implemented",
                c, l
            )))
        }
    };

    primitive::iter_to_array::<i64, _, _>(iter, descriptor, data_type)
        .map(|x| Box::new(x) as Box<dyn Array>)
}

pub fn page_iter_to_array<I: Iterator<Item = std::result::Result<CompressedPage, ParquetError>>>(
    iter: I,
    descriptor: &ColumnDescriptor,
) -> Result<Box<dyn Array>> {
    match descriptor.type_() {
        ParquetType::PrimitiveType {
            physical_type,
            converted_type,
            logical_type,
            ..
        } => match (physical_type, converted_type, logical_type) {
            // todo: apply conversion rules and the like
            (PhysicalType::Int32, None, None) => Ok(Box::new(
                primitive::iter_to_array::<i32, _, _>(iter, descriptor, DataType::Int32)?,
            )),
            (PhysicalType::Int64, _, _) => {
                page_iter_to_array_i64(iter, descriptor, converted_type, logical_type)
            }
            (PhysicalType::Float, None, None) => Ok(Box::new(
                primitive::iter_to_array::<f32, _, _>(iter, descriptor, DataType::Float32)?,
            )),
            (PhysicalType::Double, None, None) => {
                Ok(Box::new(primitive::iter_to_array::<f64, _, _>(
                    iter,
                    descriptor,
                    DataType::Float64,
                )?))
            }
            (PhysicalType::Boolean, None, None) => {
                Ok(Box::new(boolean::iter_to_array(iter, descriptor)?))
            }
            (PhysicalType::ByteArray, Some(PrimitiveConvertedType::Utf8), _) => Ok(Box::new(
                utf8::iter_to_array::<i32, _, _>(iter, descriptor)?,
            )),
            (p, c, l) => Err(ArrowError::NotYetImplemented(format!(
                "The conversion of ({:?}, {:?}, {:?}) to arrow still not implemented",
                p, c, l
            ))),
        },
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use parquet2::read::{get_page_iterator, read_metadata};

    use crate::array::*;

    use super::super::tests::pyarrow_integration;
    use super::*;

    fn get_column(path: &str, row_group: usize, column: usize) -> Result<Box<dyn Array>> {
        let mut file = File::open(path).unwrap();

        let metadata = read_metadata(&mut file)?;
        let iter = get_page_iterator(&metadata, row_group, column, &mut file)?;

        let descriptor = iter.descriptor().clone();

        page_iter_to_array(iter, &descriptor)
    }

    #[test]
    fn pyarrow_integration_v1_int64() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let column = 0;
        let path = "fixtures/pyarrow3/v1/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_v1_float64() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let column = 1;
        let path = "fixtures/pyarrow3/v1/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_v1_utf8() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let column = 2;
        let path = "fixtures/pyarrow3/v1/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_v1_boolean() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let column = 3;
        let path = "fixtures/pyarrow3/v1/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_v1_timestamp() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let column = 4;
        let path = "fixtures/pyarrow3/v1/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    #[ignore] // pyarrow issue; see https://issues.apache.org/jira/browse/ARROW-12201
    fn pyarrow_integration_v1_u32() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let column = 5;
        let path = "fixtures/pyarrow3/v1/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_v2_int64() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let column = 0;
        let path = "fixtures/pyarrow3/v2/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_v2_utf8() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let column = 2;
        let path = "fixtures/pyarrow3/v2/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn pyarrow_integration_v2_boolean() -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let column = 3;
        let path = "fixtures/pyarrow3/v2/basic_nulls_10.parquet";
        let array = get_column(path, 0, column)?;

        let expected = pyarrow_integration(column);

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }
}
