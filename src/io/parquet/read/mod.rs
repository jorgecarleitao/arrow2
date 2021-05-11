mod boolean;
mod primitive;
pub mod schema;
mod utf8;
mod utils;

use crate::{
    array::Array,
    datatypes::{DataType, TimeUnit},
    error::{ArrowError, Result},
};

pub use schema::get_schema;

pub use parquet2::read::{get_page_iterator, read_metadata};
pub use parquet2::{
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

    use crate::array::*;

    use super::super::tests::*;
    use super::*;

    fn get_column(path: &str, row_group: usize, column: usize) -> Result<Box<dyn Array>> {
        let mut file = File::open(path).unwrap();

        let metadata = read_metadata(&mut file)?;
        let iter = get_page_iterator(&metadata, row_group, column, &mut file)?;

        let descriptor = iter.descriptor().clone();

        page_iter_to_array(iter, &descriptor)
    }

    fn test_pyarrow_integration(column: usize, version: usize, required: bool) -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let path = if required {
            format!(
                "fixtures/pyarrow3/v{}/basic_{}_10.parquet",
                version, "required"
            )
        } else {
            format!(
                "fixtures/pyarrow3/v{}/basic_{}_10.parquet",
                version, "nullable"
            )
        };
        let array = get_column(&path, 0, column)?;

        let expected = if required {
            pyarrow_required(column)
        } else {
            pyarrow_nullable(column)
        };

        assert_eq!(expected.as_ref(), array.as_ref());

        Ok(())
    }

    #[test]
    fn v1_int64_nullable() -> Result<()> {
        test_pyarrow_integration(0, 1, false)
    }

    #[test]
    fn v1_float64_nullable() -> Result<()> {
        test_pyarrow_integration(1, 1, false)
    }

    #[test]
    fn v1_utf8_nullable() -> Result<()> {
        test_pyarrow_integration(2, 1, false)
    }

    #[test]
    fn v1_boolean_nullable() -> Result<()> {
        test_pyarrow_integration(3, 1, false)
    }

    #[test]
    fn v1_timestamp_nullable() -> Result<()> {
        test_pyarrow_integration(4, 1, false)
    }

    #[test]
    #[ignore] // pyarrow issue; see https://issues.apache.org/jira/browse/ARROW-12201
    fn v1_u32_nullable() -> Result<()> {
        test_pyarrow_integration(5, 1, false)
    }

    #[test]
    fn v2_int64_nullable() -> Result<()> {
        test_pyarrow_integration(0, 2, false)
    }

    #[test]
    fn v2_utf8_nullable() -> Result<()> {
        test_pyarrow_integration(2, 2, false)
    }

    #[test]
    fn v2_boolean_nullable() -> Result<()> {
        test_pyarrow_integration(3, 2, false)
    }
}
