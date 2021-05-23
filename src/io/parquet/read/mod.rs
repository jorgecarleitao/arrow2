use std::io::{Read, Seek};

mod binary;
mod boolean;
mod fixed_size_binary;
mod primitive;
pub mod schema;
mod utf8;
mod utils;

use crate::{
    array::Array,
    datatypes::{DataType, TimeUnit},
    error::{ArrowError, Result},
};

pub use schema::{get_schema, is_type_nullable, FileMetaData};

pub use parquet2::{
    error::ParquetError,
    metadata::ColumnDescriptor,
    read::CompressedPage,
    schema::{
        types::{LogicalType, ParquetType, PhysicalType, PrimitiveConvertedType},
        TimeUnit as ParquetTimeUnit, TimestampType,
    },
};
use parquet2::{
    read::{
        get_page_iterator as _get_page_iterator, read_metadata as _read_metadata, PageIterator,
    },
    types::int96_to_i64_ns,
};

/// Creates a new iterator of compressed pages.
pub fn get_page_iterator<'b, RR: Read + Seek>(
    metadata: &FileMetaData,
    row_group: usize,
    column: usize,
    reader: &'b mut RR,
) -> Result<PageIterator<'b, RR>> {
    Ok(_get_page_iterator(metadata, row_group, column, reader)?)
}

/// Reads parquets' metadata.
pub fn read_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetaData> {
    Ok(_read_metadata(reader)?)
}

fn page_iter_i64<I: Iterator<Item = std::result::Result<CompressedPage, ParquetError>>>(
    iter: I,
    descriptor: &ColumnDescriptor,
    converted_type: &Option<PrimitiveConvertedType>,
    logical_type: &Option<LogicalType>,
) -> Result<Box<dyn Array>> {
    let data_type = schema::from_int64(logical_type, converted_type)?;

    match data_type {
        DataType::UInt64 => {
            primitive::iter_to_array(iter, descriptor, data_type, |x: i64| x as u64)
                .map(|x| Box::new(x) as Box<dyn Array>)
        }
        _ => primitive::iter_to_array(iter, descriptor, data_type, |x: i64| x as i64)
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
        UInt8 => primitive::iter_to_array(iter, descriptor, data_type, |x: i32| x as u8)
            .map(|x| Box::new(x) as Box<dyn Array>),
        UInt16 => primitive::iter_to_array(iter, descriptor, data_type, |x: i32| x as u16)
            .map(|x| Box::new(x) as Box<dyn Array>),
        UInt32 => primitive::iter_to_array(iter, descriptor, data_type, |x: i32| x as u32)
            .map(|x| Box::new(x) as Box<dyn Array>),
        Int8 => primitive::iter_to_array(iter, descriptor, data_type, |x: i32| x as i8)
            .map(|x| Box::new(x) as Box<dyn Array>),
        Int16 => primitive::iter_to_array(iter, descriptor, data_type, |x: i32| x as i16)
            .map(|x| Box::new(x) as Box<dyn Array>),
        _ => primitive::iter_to_array(iter, descriptor, data_type, |x: i32| x)
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
            (PhysicalType::Float, None, None) => Ok(Box::new(primitive::iter_to_array(
                iter,
                descriptor,
                DataType::Float32,
                |x: f32| x,
            )?)),
            (PhysicalType::Double, None, None) => Ok(Box::new(primitive::iter_to_array(
                iter,
                descriptor,
                DataType::Float64,
                |x: f64| x,
            )?)),
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
            (PhysicalType::Int96, _, _) => Ok(Box::new(primitive::iter_to_array(
                iter,
                descriptor,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                int96_to_i64_ns,
            )?)),
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

#[cfg(test)]
mod tests_integration {
    use super::*;
    use crate::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn all_types() -> Result<()> {
        let path = "testing/parquet-testing/data/alltypes_plain.parquet";
        let mut reader = std::fs::File::open(path)?;

        let file_metadata = read_metadata(&mut reader)?;

        let schema = get_schema(&file_metadata)?;
        let schema = Arc::new(schema);

        // checks that record arrays and schema are consistent
        file_metadata
            .row_groups
            .iter()
            .enumerate()
            .map(|(row_group, group)| {
                let columns = group
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(column, column_meta)| {
                        let pages =
                            get_page_iterator(&file_metadata, row_group, column, &mut reader)?;
                        page_iter_to_array(pages, column_meta.descriptor()).map(|x| x.into())
                    })
                    .collect::<Result<Vec<Arc<dyn Array>>>>()?;
                RecordBatch::try_new(schema.clone(), columns)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }
}
