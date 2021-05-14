mod binary;
mod boolean;
mod fixed_size_binary;
mod primitive;
pub mod schema;
mod utf8;
mod utils;

use crate::{
    array::Array,
    datatypes::DataType,
    error::{ArrowError, Result},
};

pub use schema::{get_schema, is_type_nullable};

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

fn page_iter_i64<I: Iterator<Item = std::result::Result<CompressedPage, ParquetError>>>(
    iter: I,
    descriptor: &ColumnDescriptor,
    converted_type: &Option<PrimitiveConvertedType>,
    logical_type: &Option<LogicalType>,
) -> Result<Box<dyn Array>> {
    let data_type = schema::from_int64(logical_type, converted_type)?;

    match data_type {
        DataType::UInt64 => primitive::iter_to_array::<i64, u64, _, _>(iter, descriptor, data_type)
            .map(|x| Box::new(x) as Box<dyn Array>),
        _ => primitive::iter_to_array::<i64, i64, _, _>(iter, descriptor, data_type)
            .map(|x| Box::new(x) as Box<dyn Array>),
    }
}

fn page_iter_i32<I: Iterator<Item = std::result::Result<CompressedPage, ParquetError>>>(
    iter: I,
    descriptor: &ColumnDescriptor,
    converted_type: &Option<PrimitiveConvertedType>,
    logical_type: &Option<LogicalType>,
) -> Result<Box<dyn Array>> {
    let data_type = schema::from_int32(logical_type, converted_type)?;

    use DataType::*;
    match data_type {
        UInt8 => primitive::iter_to_array::<i32, u8, _, _>(iter, descriptor, data_type)
            .map(|x| Box::new(x) as Box<dyn Array>),
        UInt16 => primitive::iter_to_array::<i32, u16, _, _>(iter, descriptor, data_type)
            .map(|x| Box::new(x) as Box<dyn Array>),
        UInt32 => primitive::iter_to_array::<i32, u32, _, _>(iter, descriptor, data_type)
            .map(|x| Box::new(x) as Box<dyn Array>),
        Int8 => primitive::iter_to_array::<i32, i8, _, _>(iter, descriptor, data_type)
            .map(|x| Box::new(x) as Box<dyn Array>),
        Int16 => primitive::iter_to_array::<i32, i16, _, _>(iter, descriptor, data_type)
            .map(|x| Box::new(x) as Box<dyn Array>),
        _ => primitive::iter_to_array::<i32, i32, _, _>(iter, descriptor, data_type)
            .map(|x| Box::new(x) as Box<dyn Array>),
    }
}

fn page_iter_byte_array<I: Iterator<Item = std::result::Result<CompressedPage, ParquetError>>>(
    iter: I,
    descriptor: &ColumnDescriptor,
    converted_type: &Option<PrimitiveConvertedType>,
    logical_type: &Option<LogicalType>,
) -> Result<Box<dyn Array>> {
    let data_type = schema::from_byte_array(logical_type, converted_type)?;

    use DataType::*;
    Ok(match data_type {
        Utf8 => Box::new(utf8::iter_to_array::<i32, _, _>(iter, descriptor)?),
        LargeUtf8 => Box::new(utf8::iter_to_array::<i64, _, _>(iter, descriptor)?),
        Binary => Box::new(binary::iter_to_array::<i32, _, _>(iter, descriptor)?),
        LargeBinary => Box::new(binary::iter_to_array::<i64, _, _>(iter, descriptor)?),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Can't read {:?} from parquet",
                other
            )))
        }
    })
}

fn page_iter_fixed_len_byte_array<
    I: Iterator<Item = std::result::Result<CompressedPage, ParquetError>>,
>(
    iter: I,
    length: &i32,
    descriptor: &ColumnDescriptor,
    converted_type: &Option<PrimitiveConvertedType>,
    logical_type: &Option<LogicalType>,
) -> Result<Box<dyn Array>> {
    let data_type = schema::from_fixed_len_byte_array(length, logical_type, converted_type);

    use DataType::*;
    Ok(match data_type {
        FixedSizeBinary(size) => {
            Box::new(fixed_size_binary::iter_to_array(iter, size, descriptor)?)
        }
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Can't read {:?} from parquet",
                other
            )))
        }
    })
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
            (PhysicalType::Int32, _, _) => {
                page_iter_i32(iter, descriptor, converted_type, logical_type)
            }
            (PhysicalType::Int64, _, _) => {
                page_iter_i64(iter, descriptor, converted_type, logical_type)
            }
            (PhysicalType::Float, None, None) => {
                Ok(Box::new(primitive::iter_to_array::<f32, f32, _, _>(
                    iter,
                    descriptor,
                    DataType::Float32,
                )?))
            }
            (PhysicalType::Double, None, None) => {
                Ok(Box::new(primitive::iter_to_array::<f64, f64, _, _>(
                    iter,
                    descriptor,
                    DataType::Float64,
                )?))
            }
            (PhysicalType::Boolean, None, None) => {
                Ok(Box::new(boolean::iter_to_array(iter, descriptor)?))
            }
            (PhysicalType::ByteArray, _, _) => {
                page_iter_byte_array(iter, descriptor, converted_type, logical_type)
            }
            (PhysicalType::FixedLenByteArray(length), _, _) => page_iter_fixed_len_byte_array(
                iter,
                length,
                descriptor,
                converted_type,
                logical_type,
            ),
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
    fn v1_int64_required() -> Result<()> {
        test_pyarrow_integration(0, 1, true)
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
    fn v1_utf8_required() -> Result<()> {
        test_pyarrow_integration(2, 1, true)
    }

    #[test]
    fn v1_boolean_nullable() -> Result<()> {
        test_pyarrow_integration(3, 1, false)
    }

    #[test]
    fn v1_boolean_required() -> Result<()> {
        test_pyarrow_integration(3, 1, true)
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
    fn v2_utf8_required() -> Result<()> {
        test_pyarrow_integration(2, 2, true)
    }

    #[test]
    fn v2_boolean_nullable() -> Result<()> {
        test_pyarrow_integration(3, 2, false)
    }

    #[test]
    fn v2_boolean_required() -> Result<()> {
        test_pyarrow_integration(3, 2, true)
    }
}
