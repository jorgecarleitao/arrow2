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
    datatypes::{DataType, IntervalUnit, TimeUnit},
    error::{ArrowError, Result},
};

pub use record_batch::RecordReader;
pub use schema::{get_schema, is_type_nullable, FileMetaData};

pub use parquet2::{
    error::ParquetError,
    metadata::{ColumnChunkMetaData, ColumnDescriptor, RowGroupMetaData},
    read::{
        decompress, get_page_iterator as _get_page_iterator, read_metadata as _read_metadata,
        streaming_iterator, CompressedPage, Decompressor, Page, PageHeader, PageIterator,
        StreamingIterator,
    },
    schema::{
        types::{LogicalType, ParquetType, PhysicalType, PrimitiveConvertedType},
        TimeUnit as ParquetTimeUnit, TimestampType,
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

pub fn page_iter_to_array<I: StreamingIterator<Item = std::result::Result<Page, ParquetError>>>(
    iter: &mut I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>> {
    use DataType::*;
    match data_type {
        // INT32
        UInt8 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as u8),
        UInt16 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as u16),
        UInt32 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as u32),
        Int8 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as i8),
        Int16 => primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as i16),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => {
            primitive::iter_to_array(iter, metadata, data_type, |x: i32| x as i32)
        }

        Timestamp(TimeUnit::Nanosecond, None) => match metadata.descriptor().type_() {
            ParquetType::PrimitiveType { physical_type, .. } => match physical_type {
                PhysicalType::Int96 => primitive::iter_to_array(
                    iter,
                    metadata,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    int96_to_i64_ns,
                ),
                _ => primitive::iter_to_array(iter, metadata, data_type, |x: i64| x),
            },
            _ => unreachable!(),
        },

        // INT64
        Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => {
            primitive::iter_to_array(iter, metadata, data_type, |x: i64| x)
        }
        UInt64 => primitive::iter_to_array(iter, metadata, data_type, |x: i64| x as u64),

        Float32 => primitive::iter_to_array(iter, metadata, data_type, |x: f32| x),
        Float64 => primitive::iter_to_array(iter, metadata, data_type, |x: f64| x),

        Boolean => Ok(Box::new(boolean::iter_to_array(iter, metadata)?)),

        Binary | Utf8 => binary::iter_to_array::<i32, _, _>(iter, metadata, &data_type),
        LargeBinary | LargeUtf8 => binary::iter_to_array::<i64, _, _>(iter, metadata, &data_type),
        FixedSizeBinary(size) => Ok(Box::new(fixed_size_binary::iter_to_array(
            iter, size, metadata,
        )?)),

        List(ref inner) => match inner.data_type() {
            UInt8 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as u8),
            UInt16 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as u16),
            UInt32 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as u32),
            Int8 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as i8),
            Int16 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as i16),
            Int32 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i32| x as i32),

            Timestamp(TimeUnit::Nanosecond, None) => match metadata.descriptor().type_() {
                ParquetType::PrimitiveType { physical_type, .. } => match physical_type {
                    PhysicalType::Int96 => primitive::iter_to_array_nested(
                        iter,
                        metadata,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        int96_to_i64_ns,
                    ),
                    _ => primitive::iter_to_array(iter, metadata, data_type, |x: i64| x),
                },
                _ => unreachable!(),
            },

            // INT64
            Int64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => {
                primitive::iter_to_array_nested(iter, metadata, data_type, |x: i64| x)
            }
            UInt64 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: i64| x as u64),

            Float32 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: f32| x),
            Float64 => primitive::iter_to_array_nested(iter, metadata, data_type, |x: f64| x),

            Boolean => boolean::iter_to_array_nested(iter, metadata, data_type),

            Binary | Utf8 => binary::iter_to_array_nested::<i32, _, _>(iter, metadata, data_type),
            LargeBinary | LargeUtf8 => {
                binary::iter_to_array_nested::<i64, _, _>(iter, metadata, data_type)
            }
            other => Err(ArrowError::NotYetImplemented(format!(
                "The conversion of {:?} to arrow still not implemented",
                other
            ))),
        },
        other => Err(ArrowError::NotYetImplemented(format!(
            "The conversion of {:?} to arrow still not implemented",
            other
        ))),
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
        use_dict: bool,
        required: bool,
    ) -> Result<()> {
        if std::env::var("ARROW2_IGNORE_PARQUET").is_ok() {
            return Ok(());
        }
        let use_dict = if use_dict { "dict/" } else { "" };
        let path = if required {
            format!(
                "fixtures/pyarrow3/v{}/{}{}_{}_10.parquet",
                version, use_dict, type_, "required"
            )
        } else {
            format!(
                "fixtures/pyarrow3/v{}/{}{}_{}_10.parquet",
                version, use_dict, type_, "nullable"
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
        test_pyarrow_integration(0, 1, "basic", false, false)
    }

    #[test]
    fn v1_int64_required() -> Result<()> {
        test_pyarrow_integration(0, 1, "basic", false, true)
    }

    #[test]
    fn v1_float64_nullable() -> Result<()> {
        test_pyarrow_integration(1, 1, "basic", false, false)
    }

    #[test]
    fn v1_utf8_nullable() -> Result<()> {
        test_pyarrow_integration(2, 1, "basic", false, false)
    }

    #[test]
    fn v1_utf8_required() -> Result<()> {
        test_pyarrow_integration(2, 1, "basic", false, true)
    }

    #[test]
    fn v1_boolean_nullable() -> Result<()> {
        test_pyarrow_integration(3, 1, "basic", false, false)
    }

    #[test]
    fn v1_boolean_required() -> Result<()> {
        test_pyarrow_integration(3, 1, "basic", false, true)
    }

    #[test]
    fn v1_timestamp_nullable() -> Result<()> {
        test_pyarrow_integration(4, 1, "basic", false, false)
    }

    #[test]
    #[ignore] // pyarrow issue; see https://issues.apache.org/jira/browse/ARROW-12201
    fn v1_u32_nullable() -> Result<()> {
        test_pyarrow_integration(5, 1, "basic", false, false)
    }

    #[test]
    fn v2_int64_nullable() -> Result<()> {
        test_pyarrow_integration(0, 2, "basic", false, false)
    }

    #[test]
    fn v2_int64_nullable_dict() -> Result<()> {
        test_pyarrow_integration(0, 2, "basic", true, false)
    }

    #[test]
    fn v1_int64_nullable_dict() -> Result<()> {
        test_pyarrow_integration(0, 1, "basic", true, false)
    }

    #[test]
    fn v2_utf8_nullable() -> Result<()> {
        test_pyarrow_integration(2, 2, "basic", false, false)
    }

    #[test]
    fn v2_utf8_required() -> Result<()> {
        test_pyarrow_integration(2, 2, "basic", false, true)
    }

    #[test]
    fn v2_utf8_nullable_dict() -> Result<()> {
        test_pyarrow_integration(2, 2, "basic", true, false)
    }

    #[test]
    fn v1_utf8_nullable_dict() -> Result<()> {
        test_pyarrow_integration(2, 1, "basic", true, false)
    }

    #[test]
    fn v2_boolean_nullable() -> Result<()> {
        test_pyarrow_integration(3, 2, "basic", false, false)
    }

    #[test]
    fn v2_boolean_required() -> Result<()> {
        test_pyarrow_integration(3, 2, "basic", false, true)
    }

    #[test]
    fn v2_nested_int64_nullable() -> Result<()> {
        test_pyarrow_integration(0, 2, "nested", false, false)
    }

    #[test]
    fn v1_nested_int64_nullable() -> Result<()> {
        test_pyarrow_integration(0, 1, "nested", false, false)
    }

    #[test]
    fn v2_nested_int64_nullable_required() -> Result<()> {
        test_pyarrow_integration(1, 2, "nested", false, false)
    }

    #[test]
    fn v1_nested_int64_nullable_required() -> Result<()> {
        test_pyarrow_integration(1, 1, "nested", false, false)
    }

    #[test]
    fn v2_nested_int64_required_required() -> Result<()> {
        test_pyarrow_integration(2, 2, "nested", false, false)
    }

    #[test]
    fn v1_nested_int64_required_required() -> Result<()> {
        test_pyarrow_integration(2, 1, "nested", false, false)
    }

    #[test]
    fn v2_nested_i16() -> Result<()> {
        test_pyarrow_integration(3, 2, "nested", false, false)
    }

    #[test]
    fn v1_nested_i16() -> Result<()> {
        test_pyarrow_integration(3, 1, "nested", false, false)
    }

    #[test]
    fn v2_nested_bool() -> Result<()> {
        test_pyarrow_integration(4, 2, "nested", false, false)
    }

    #[test]
    fn v1_nested_bool() -> Result<()> {
        test_pyarrow_integration(4, 1, "nested", false, false)
    }

    #[test]
    fn v2_nested_utf8() -> Result<()> {
        test_pyarrow_integration(5, 2, "nested", false, false)
    }

    #[test]
    fn v1_nested_utf8() -> Result<()> {
        test_pyarrow_integration(5, 1, "nested", false, false)
    }

    #[test]
    fn v2_nested_large_binary() -> Result<()> {
        test_pyarrow_integration(6, 2, "nested", false, false)
    }

    #[test]
    fn v1_nested_large_binary() -> Result<()> {
        test_pyarrow_integration(6, 1, "nested", false, false)
    }

    /*#[test]
    fn v2_nested_nested() {
        let _ = test_pyarrow_integration(7, 1, "nested",false, false);
    }*/
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
