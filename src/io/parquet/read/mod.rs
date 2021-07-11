use std::io::{Read, Seek};

mod binary;
mod boolean;
mod fixed_size_binary;
mod nested_utils;
mod primitive;
mod record_batch;
pub mod schema;
pub mod statistics;
mod utils;

use crate::{
    array::Array,
    datatypes::{DataType, TimeUnit},
    error::{ArrowError, Result},
};

pub use record_batch::RecordReader;
pub use schema::{get_schema, is_type_nullable, FileMetaData};

pub use parquet2::{
    error::ParquetError,
    metadata::{ColumnChunkMetaData, ColumnDescriptor, RowGroupMetaData},
    read::{
        decompress, streaming_iterator, CompressedPage, Decompressor, Page, PageHeader,
        StreamingIterator,
    },
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
    buffer: Vec<u8>,
) -> Result<PageIterator<'b, RR>> {
    Ok(_get_page_iterator(
        metadata, row_group, column, reader, buffer,
    )?)
}

/// Reads parquets' metadata.
pub fn read_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetaData> {
    Ok(_read_metadata(reader)?)
}

fn page_iter_i64<I: StreamingIterator<Item = std::result::Result<Page, ParquetError>>>(
    iter: I,
    metadata: &ColumnChunkMetaData,
    converted_type: &Option<PrimitiveConvertedType>,
    logical_type: &Option<LogicalType>,
) -> Result<Box<dyn Array>> {
    let data_type = schema::from_int64(logical_type, converted_type)?;

    let is_nested = metadata.descriptor().max_rep_level() > 0;

    if is_nested {
        let real_type = schema::to_data_type(metadata.descriptor().base_type())?.unwrap();

        match data_type {
            DataType::UInt64 => {
                primitive::iter_to_array_nested(iter, metadata, real_type, |x: i64| x as u64)
            }
            _ => primitive::iter_to_array_nested(iter, metadata, real_type, |x: i64| x as i64),
        }
    } else {
        match data_type {
            DataType::UInt64 => {
                primitive::iter_to_array(iter, metadata, data_type, |x: i64| x as u64)
            }
            _ => primitive::iter_to_array(iter, metadata, data_type, |x: i64| x as i64),
        }
    }
}

fn page_iter_i32<I: StreamingIterator<Item = std::result::Result<Page, ParquetError>>>(
    iter: I,
    metadata: &ColumnChunkMetaData,
    converted_type: &Option<PrimitiveConvertedType>,
    logical_type: &Option<LogicalType>,
) -> Result<Box<dyn Array>> {
    let data_type = schema::from_int32(logical_type, converted_type)?;

    use DataType::*;

    if metadata.descriptor().max_rep_level() > 0 {
        let real_type = schema::to_data_type(metadata.descriptor().base_type())?.unwrap();

        match data_type {
            UInt8 => primitive::iter_to_array_nested(iter, metadata, real_type, |x: i32| x as u8),
            UInt16 => primitive::iter_to_array_nested(iter, metadata, real_type, |x: i32| x as u16),
            UInt32 => primitive::iter_to_array_nested(iter, metadata, real_type, |x: i32| x as u32),
            Int8 => primitive::iter_to_array_nested(iter, metadata, real_type, |x: i32| x as i8),
            Int16 => primitive::iter_to_array_nested(iter, metadata, real_type, |x: i32| x as i16),
            _ => primitive::iter_to_array_nested(iter, metadata, real_type, |x: i32| x),
        }
    } else {
        match data_type {
            UInt8 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as u8),
            UInt16 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as u16),
            UInt32 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as u32),
            Int8 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as i8),
            Int16 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as i16),
            _ => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x),
        }
    }
}

fn page_iter_byte_array<I: StreamingIterator<Item = std::result::Result<Page, ParquetError>>>(
    iter: I,
    metadata: &ColumnChunkMetaData,
    converted_type: &Option<PrimitiveConvertedType>,
    logical_type: &Option<LogicalType>,
) -> Result<Box<dyn Array>> {
    let data_type = schema::from_byte_array(logical_type, converted_type)?;

    if metadata.descriptor().max_rep_level() > 0 {
        let real_type = schema::to_data_type(metadata.descriptor().base_type())?.unwrap();

        use DataType::*;
        match data_type {
            Binary | Utf8 => binary::iter_to_array_nested::<i32, _, _>(iter, metadata, real_type),
            LargeBinary | LargeUtf8 => {
                binary::iter_to_array_nested::<i64, _, _>(iter, metadata, real_type)
            }
            other => Err(ArrowError::NotYetImplemented(format!(
                "Can't read {:?} from parquet",
                other
            ))),
        }
    } else {
        use DataType::*;
        match data_type {
            Binary | Utf8 => binary::iter_to_array::<i32, _, _>(iter, metadata, &data_type),
            LargeBinary | LargeUtf8 => {
                binary::iter_to_array::<i64, _, _>(iter, metadata, &data_type)
            }
            other => Err(ArrowError::NotYetImplemented(format!(
                "Can't read {:?} from parquet",
                other
            ))),
        }
    }
}

fn page_iter_fixed_len_byte_array<
    I: StreamingIterator<Item = std::result::Result<Page, ParquetError>>,
>(
    iter: I,
    length: &i32,
    metadata: &ColumnChunkMetaData,
    converted_type: &Option<PrimitiveConvertedType>,
    logical_type: &Option<LogicalType>,
) -> Result<Box<dyn Array>> {
    let data_type = schema::from_fixed_len_byte_array(length, logical_type, converted_type);

    use DataType::*;
    Ok(match data_type {
        FixedSizeBinary(size) => Box::new(fixed_size_binary::iter_to_array(iter, size, metadata)?),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Can't read {:?} from parquet",
                other
            )))
        }
    })
}

pub fn page_iter_to_array<I: StreamingIterator<Item = std::result::Result<Page, ParquetError>>>(
    iter: &mut I,
    metadata: &ColumnChunkMetaData,
) -> Result<Box<dyn Array>> {
    match metadata.descriptor().base_type() {
        ParquetType::PrimitiveType {
            physical_type,
            converted_type,
            logical_type,
            ..
        } => match (physical_type, converted_type, logical_type) {
            (PhysicalType::Int32, _, _) => {
                page_iter_i32(iter, metadata, converted_type, logical_type)
            }
            (PhysicalType::Int64, _, _) => {
                page_iter_i64(iter, metadata, converted_type, logical_type)
            }
            (PhysicalType::Float, None, None) => {
                primitive::iter_to_array(iter, metadata, DataType::Float32, |x: f32| x)
            }
            (PhysicalType::Double, None, None) => {
                primitive::iter_to_array(iter, metadata, DataType::Float64, |x: f64| x)
            }
            (PhysicalType::Boolean, None, None) => {
                Ok(Box::new(boolean::iter_to_array(iter, metadata)?))
            }
            (PhysicalType::ByteArray, _, _) => {
                page_iter_byte_array(iter, metadata, converted_type, logical_type)
            }
            (PhysicalType::FixedLenByteArray(length), _, _) => {
                page_iter_fixed_len_byte_array(iter, length, metadata, converted_type, logical_type)
            }
            (PhysicalType::Int96, _, _) => primitive::iter_to_array(
                iter,
                metadata,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                int96_to_i64_ns,
            ),
            (p, c, l) => Err(ArrowError::NotYetImplemented(format!(
                "The conversion of ({:?}, {:?}, {:?}) to arrow still not implemented",
                p, c, l
            ))),
        },
        _ => {
            // get all primitives in the type_ and their max rep levels
            match metadata.descriptor().type_() {
                ParquetType::PrimitiveType {
                    physical_type,
                    converted_type,
                    logical_type,
                    ..
                } => match (physical_type, converted_type, logical_type) {
                    (PhysicalType::Boolean, None, None) => {
                        let real_type =
                            schema::to_data_type(metadata.descriptor().base_type())?.unwrap();
                        boolean::iter_to_array_nested(iter, metadata, real_type)
                    }
                    (PhysicalType::Int64, _, _) => {
                        page_iter_i64(iter, metadata, converted_type, logical_type)
                    }
                    (PhysicalType::Int32, _, _) => {
                        page_iter_i32(iter, metadata, converted_type, logical_type)
                    }
                    (PhysicalType::ByteArray, _, _) => {
                        page_iter_byte_array(iter, metadata, converted_type, logical_type)
                    }
                    _ => todo!(),
                },
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::super::tests::*;
    use super::*;

    fn test_pyarrow_integration(
        column: usize,
        version: usize,
        type_: &str,
        required: bool,
    ) -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let path = if required {
            format!(
                "fixtures/pyarrow3/v{}/{}_{}_10.parquet",
                version, type_, "required"
            )
        } else {
            format!(
                "fixtures/pyarrow3/v{}/{}_{}_10.parquet",
                version, type_, "nullable"
            )
        };
        let mut file = File::open(path).unwrap();
        let (array, statistics) = read_column(&mut file, 0, column)?;

        let expected = match (type_, required) {
            ("basic", true) => pyarrow_required(column),
            ("basic", false) => pyarrow_nullable(column),
            ("nested", false) => pyarrow_nested_nullable(column),
            _ => unreachable!(),
        };

        let expected_statistics = match (type_, required) {
            ("basic", true) => pyarrow_required_statistics(column),
            ("basic", false) => pyarrow_nullable_statistics(column),
            ("nested", false) => pyarrow_nested_nullable_statistics(column),
            _ => unreachable!(),
        };

        assert_eq!(expected.as_ref(), array.as_ref());
        assert_eq!(expected_statistics.as_ref(), statistics.unwrap().as_ref());

        Ok(())
    }

    #[test]
    fn v1_int64_nullable() -> Result<()> {
        test_pyarrow_integration(0, 1, "basic", false)
    }

    #[test]
    fn v1_int64_required() -> Result<()> {
        test_pyarrow_integration(0, 1, "basic", true)
    }

    #[test]
    fn v1_float64_nullable() -> Result<()> {
        test_pyarrow_integration(1, 1, "basic", false)
    }

    #[test]
    fn v1_utf8_nullable() -> Result<()> {
        test_pyarrow_integration(2, 1, "basic", false)
    }

    #[test]
    fn v1_utf8_required() -> Result<()> {
        test_pyarrow_integration(2, 1, "basic", true)
    }

    #[test]
    fn v1_boolean_nullable() -> Result<()> {
        test_pyarrow_integration(3, 1, "basic", false)
    }

    #[test]
    fn v1_boolean_required() -> Result<()> {
        test_pyarrow_integration(3, 1, "basic", true)
    }

    #[test]
    fn v1_timestamp_nullable() -> Result<()> {
        test_pyarrow_integration(4, 1, "basic", false)
    }

    #[test]
    #[ignore] // pyarrow issue; see https://issues.apache.org/jira/browse/ARROW-12201
    fn v1_u32_nullable() -> Result<()> {
        test_pyarrow_integration(5, 1, "basic", false)
    }

    #[test]
    fn v2_int64_nullable() -> Result<()> {
        test_pyarrow_integration(0, 2, "basic", false)
    }

    #[test]
    fn v2_utf8_nullable() -> Result<()> {
        test_pyarrow_integration(2, 2, "basic", false)
    }

    #[test]
    fn v2_utf8_required() -> Result<()> {
        test_pyarrow_integration(2, 2, "basic", true)
    }

    #[test]
    fn v2_boolean_nullable() -> Result<()> {
        test_pyarrow_integration(3, 2, "basic", false)
    }

    #[test]
    fn v2_boolean_required() -> Result<()> {
        test_pyarrow_integration(3, 2, "basic", true)
    }

    #[test]
    fn v2_nested_int64_nullable() -> Result<()> {
        test_pyarrow_integration(0, 2, "nested", false)
    }

    #[test]
    fn v1_nested_int64_nullable() -> Result<()> {
        test_pyarrow_integration(0, 1, "nested", false)
    }

    #[test]
    fn v2_nested_int64_nullable_required() -> Result<()> {
        test_pyarrow_integration(1, 2, "nested", false)
    }

    #[test]
    fn v1_nested_int64_nullable_required() -> Result<()> {
        test_pyarrow_integration(1, 1, "nested", false)
    }

    #[test]
    fn v2_nested_int64_required_required() -> Result<()> {
        test_pyarrow_integration(2, 2, "nested", false)
    }

    #[test]
    fn v1_nested_int64_required_required() -> Result<()> {
        test_pyarrow_integration(2, 1, "nested", false)
    }

    #[test]
    fn v2_nested_i16() -> Result<()> {
        test_pyarrow_integration(3, 2, "nested", false)
    }

    #[test]
    fn v1_nested_i16() -> Result<()> {
        test_pyarrow_integration(3, 1, "nested", false)
    }

    #[test]
    fn v2_nested_bool() -> Result<()> {
        test_pyarrow_integration(4, 2, "nested", false)
    }

    #[test]
    fn v1_nested_bool() -> Result<()> {
        test_pyarrow_integration(4, 1, "nested", false)
    }

    #[test]
    fn v2_nested_utf8() -> Result<()> {
        test_pyarrow_integration(5, 2, "nested", false)
    }

    #[test]
    fn v1_nested_utf8() -> Result<()> {
        test_pyarrow_integration(5, 1, "nested", false)
    }
}

#[cfg(test)]
mod tests_integration {
    use crate::array::{BinaryArray, Float32Array, Int32Array};

    use super::*;
    use std::sync::Arc;

    #[test]
    fn all_types() -> Result<()> {
        let path = "testing/parquet-testing/data/alltypes_plain.parquet";
        let reader = std::fs::File::open(path)?;

        let reader = RecordReader::try_new(reader, None, None, Arc::new(|_, _| true))?;

        let batches = reader.collect::<Result<Vec<_>>>()?;
        assert_eq!(batches.len(), 1);

        let result = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(result, &Int32Array::from_slice([4, 5, 6, 7, 2, 3, 0, 1]));

        let result = batches[0]
            .column(6)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(
            result,
            &Float32Array::from_slice([0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1])
        );

        let result = batches[0]
            .column(9)
            .as_any()
            .downcast_ref::<BinaryArray<i32>>()
            .unwrap();
        assert_eq!(
            result,
            &BinaryArray::<i32>::from_slice([[48], [49], [48], [49], [48], [49], [48], [49]])
        );

        Ok(())
    }
}
