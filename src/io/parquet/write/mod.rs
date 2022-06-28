//! APIs to write to Parquet format.
mod binary;
mod boolean;
mod dictionary;
mod file;
mod fixed_len_bytes;
mod nested;
mod pages;
mod primitive;
mod row_group;
mod schema;
mod sink;
mod utf8;
mod utils;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{Error, Result};
use crate::types::days_ms;
use crate::types::NativeType;

use parquet2::schema::types::PrimitiveType as ParquetPrimitiveType;
pub use parquet2::{
    compression::{BrotliLevel, CompressionLevel, CompressionOptions, GzipLevel, ZstdLevel},
    encoding::Encoding,
    fallible_streaming_iterator,
    metadata::{Descriptor, FileMetaData, KeyValue, SchemaDescriptor, ThriftFileMetaData},
    page::{CompressedDataPage, CompressedPage, EncodedPage},
    schema::types::{FieldInfo, ParquetType, PhysicalType as ParquetPhysicalType},
    write::{
        compress, write_metadata_sidecar, Compressor, DynIter, DynStreamingIterator, RowGroupIter,
        Version,
    },
    FallibleStreamingIterator,
};

/// Currently supported options to write to parquet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    /// Whether to write statistics
    pub write_statistics: bool,
    /// The page and file version to use
    pub version: Version,
    /// The compression to apply to every page
    pub compression: CompressionOptions,
}

use crate::compute::aggregate::estimated_bytes_size;
pub use file::FileWriter;
pub use row_group::{row_group_iter, RowGroupIterator};
pub use schema::to_parquet_type;
pub use sink::FileSink;

pub use pages::array_to_columns;
pub use pages::Nested;

pub(self) fn decimal_length_from_precision(precision: usize) -> usize {
    // digits = floor(log_10(2^(8*n - 1) - 1))
    // ceil(digits) = log10(2^(8*n - 1) - 1)
    // 10^ceil(digits) = 2^(8*n - 1) - 1
    // 10^ceil(digits) + 1 = 2^(8*n - 1)
    // log2(10^ceil(digits) + 1) = (8*n - 1)
    // log2(10^ceil(digits) + 1) + 1 = 8*n
    // (log2(10^ceil(a) + 1) + 1) / 8 = n
    (((10.0_f64.powi(precision as i32) + 1.0).log2() + 1.0) / 8.0).ceil() as usize
}

/// Creates a parquet [`SchemaDescriptor`] from a [`Schema`].
pub fn to_parquet_schema(schema: &Schema) -> Result<SchemaDescriptor> {
    let parquet_types = schema
        .fields
        .iter()
        .map(to_parquet_type)
        .collect::<Result<Vec<_>>>()?;
    Ok(SchemaDescriptor::new("root".to_string(), parquet_types))
}

/// Checks whether the `data_type` can be encoded as `encoding`.
/// Note that this is whether this implementation supports it, which is a subset of
/// what the parquet spec allows.
pub fn can_encode(data_type: &DataType, encoding: Encoding) -> bool {
    matches!(
        (encoding, data_type),
        (Encoding::Plain, _)
            | (
                Encoding::DeltaLengthByteArray,
                DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8,
            )
            | (Encoding::RleDictionary, DataType::Dictionary(_, _, _))
            | (Encoding::PlainDictionary, DataType::Dictionary(_, _, _))
    )
}

/// Returns an iterator of [`EncodedPage`].
pub fn array_to_pages(
    array: &dyn Array,
    type_: ParquetPrimitiveType,
    nested: Vec<Nested>,
    options: WriteOptions,
    encoding: Encoding,
) -> Result<DynIter<'static, Result<EncodedPage>>> {
    // maximum page size is 2^31 e.g. i32::MAX
    // we split at 2^31 - 2^25 to err on the safe side
    // we also check for an array.len > 3 to prevent infinite recursion
    // still have to figure out how to deal with values that are i32::MAX size, such as very large
    // strings or a list column with many elements
    if (estimated_bytes_size(array)) >= (2u32.pow(31) - 2u32.pow(25)) as usize && array.len() > 3 {
        let split_at = array.len() / 2;
        let left = array.slice(0, split_at);
        let right = array.slice(split_at, array.len() - split_at);

        Ok(DynIter::new(
            array_to_pages(&*left, type_.clone(), nested.clone(), options, encoding)?
                .chain(array_to_pages(&*right, type_, nested, options, encoding)?),
        ))
    } else {
        match array.data_type() {
            DataType::Dictionary(key_type, _, _) => {
                match_integer_type!(key_type, |$T| {
                    dictionary::array_to_pages::<$T>(
                        array.as_any().downcast_ref().unwrap(),
                        type_,
                        options,
                        encoding,
                    )
                })
            }
            _ => array_to_page(array, type_, nested, options, encoding)
                .map(|page| DynIter::new(std::iter::once(Ok(page)))),
        }
    }
}

/// Converts an [`Array`] to a [`CompressedPage`] based on options, descriptor and `encoding`.
pub fn array_to_page(
    array: &dyn Array,
    type_: ParquetPrimitiveType,
    nested: Vec<Nested>,
    options: WriteOptions,
    encoding: Encoding,
) -> Result<EncodedPage> {
    if nested.len() == 1 {
        // special case where validity == def levels
        return array_to_page_simple(array, type_, options, encoding);
    }
    array_to_page_nested(array, type_, nested, options, encoding)
}

/// Converts an [`Array`] to a [`CompressedPage`] based on options, descriptor and `encoding`.
pub fn array_to_page_simple(
    array: &dyn Array,
    type_: ParquetPrimitiveType,
    options: WriteOptions,
    encoding: Encoding,
) -> Result<EncodedPage> {
    let data_type = array.data_type();
    if !can_encode(data_type, encoding) {
        return Err(Error::InvalidArgumentError(format!(
            "The datatype {:?} cannot be encoded by {:?}",
            data_type, encoding
        )));
    }

    match data_type.to_logical_type() {
        DataType::Boolean => {
            boolean::array_to_page(array.as_any().downcast_ref().unwrap(), options, type_)
        }
        // casts below MUST match the casts done at the metadata (field -> parquet type).
        DataType::UInt8 => primitive::array_to_page::<u8, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
        ),
        DataType::UInt16 => primitive::array_to_page::<u16, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
        ),
        DataType::UInt32 => primitive::array_to_page::<u32, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
        ),
        DataType::UInt64 => primitive::array_to_page::<u64, i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
        ),
        DataType::Int8 => primitive::array_to_page::<i8, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
        ),
        DataType::Int16 => primitive::array_to_page::<i16, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
        ),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            primitive::array_to_page::<i32, i32>(
                array.as_any().downcast_ref().unwrap(),
                options,
                type_,
            )
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => primitive::array_to_page::<i64, i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
        ),
        DataType::Float32 => primitive::array_to_page::<f32, f32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
        ),
        DataType::Float64 => primitive::array_to_page::<f64, f64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
        ),
        DataType::Utf8 => utf8::array_to_page::<i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
            encoding,
        ),
        DataType::LargeUtf8 => utf8::array_to_page::<i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
            encoding,
        ),
        DataType::Binary => binary::array_to_page::<i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
            encoding,
        ),
        DataType::LargeBinary => binary::array_to_page::<i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            type_,
            encoding,
        ),
        DataType::Null => {
            let array = Int32Array::new_null(DataType::Int32, array.len());
            primitive::array_to_page::<i32, i32>(&array, options, type_)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let type_ = type_;
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            let mut values = Vec::<u8>::with_capacity(12 * array.len());
            array.values().iter().for_each(|x| {
                let bytes = &x.to_le_bytes();
                values.extend_from_slice(bytes);
                values.extend_from_slice(&[0; 8]);
            });
            let array = FixedSizeBinaryArray::new(
                DataType::FixedSizeBinary(12),
                values.into(),
                array.validity().cloned(),
            );
            let statistics = if options.write_statistics {
                Some(fixed_len_bytes::build_statistics(&array, type_.clone()))
            } else {
                None
            };
            fixed_len_bytes::array_to_page(&array, options, type_, statistics)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let type_ = type_;
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<days_ms>>()
                .unwrap();
            let mut values = Vec::<u8>::with_capacity(12 * array.len());
            array.values().iter().for_each(|x| {
                let bytes = &x.to_le_bytes();
                values.extend_from_slice(&[0; 4]); // months
                values.extend_from_slice(bytes); // days and seconds
            });
            let array = FixedSizeBinaryArray::from_data(
                DataType::FixedSizeBinary(12),
                values.into(),
                array.validity().cloned(),
            );
            let statistics = if options.write_statistics {
                Some(fixed_len_bytes::build_statistics(&array, type_.clone()))
            } else {
                None
            };
            fixed_len_bytes::array_to_page(&array, options, type_, statistics)
        }
        DataType::FixedSizeBinary(_) => {
            let type_ = type_;
            let array = array.as_any().downcast_ref().unwrap();
            let statistics = if options.write_statistics {
                Some(fixed_len_bytes::build_statistics(array, type_.clone()))
            } else {
                None
            };

            fixed_len_bytes::array_to_page(array, options, type_, statistics)
        }
        DataType::Decimal(precision, _) => {
            let type_ = type_;
            let precision = *precision;
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i128>>()
                .unwrap();
            if precision <= 9 {
                let values = array
                    .values()
                    .iter()
                    .map(|x| *x as i32)
                    .collect::<Vec<_>>()
                    .into();

                let array =
                    PrimitiveArray::<i32>::new(DataType::Int32, values, array.validity().cloned());
                primitive::array_to_page::<i32, i32>(&array, options, type_)
            } else if precision <= 18 {
                let values = array
                    .values()
                    .iter()
                    .map(|x| *x as i64)
                    .collect::<Vec<_>>()
                    .into();

                let array =
                    PrimitiveArray::<i64>::new(DataType::Int64, values, array.validity().cloned());
                primitive::array_to_page::<i64, i64>(&array, options, type_)
            } else {
                let size = decimal_length_from_precision(precision);

                let statistics = if options.write_statistics {
                    let stats =
                        fixed_len_bytes::build_statistics_decimal(array, type_.clone(), size);
                    Some(stats)
                } else {
                    None
                };

                let mut values = Vec::<u8>::with_capacity(size * array.len());
                array.values().iter().for_each(|x| {
                    let bytes = &x.to_be_bytes()[16 - size..];
                    values.extend_from_slice(bytes)
                });
                let array = FixedSizeBinaryArray::from_data(
                    DataType::FixedSizeBinary(size),
                    values.into(),
                    array.validity().cloned(),
                );
                fixed_len_bytes::array_to_page(&array, options, type_, statistics)
            }
        }
        other => Err(Error::NotYetImplemented(format!(
            "Writing parquet pages for data type {:?}",
            other
        ))),
    }
    .map(EncodedPage::Data)
}

fn array_to_page_nested(
    array: &dyn Array,
    type_: ParquetPrimitiveType,
    nested: Vec<Nested>,
    options: WriteOptions,
    _encoding: Encoding,
) -> Result<EncodedPage> {
    use DataType::*;
    match array.data_type().to_logical_type() {
        Null => {
            let array = Int32Array::new_null(DataType::Int32, array.len());
            primitive::nested_array_to_page::<i32, i32>(&array, options, type_, nested)
        }
        Boolean => {
            let array = array.as_any().downcast_ref().unwrap();
            boolean::nested_array_to_page(array, options, type_, nested)
        }
        Utf8 => {
            let array = array.as_any().downcast_ref().unwrap();
            utf8::nested_array_to_page::<i32>(array, options, type_, nested)
        }
        LargeUtf8 => {
            let array = array.as_any().downcast_ref().unwrap();
            utf8::nested_array_to_page::<i64>(array, options, type_, nested)
        }
        Binary => {
            let array = array.as_any().downcast_ref().unwrap();
            binary::nested_array_to_page::<i32>(array, options, type_, nested)
        }
        LargeBinary => {
            let array = array.as_any().downcast_ref().unwrap();
            binary::nested_array_to_page::<i64>(array, options, type_, nested)
        }
        UInt8 => {
            let array = array.as_any().downcast_ref().unwrap();
            primitive::nested_array_to_page::<u8, i32>(array, options, type_, nested)
        }
        UInt16 => {
            let array = array.as_any().downcast_ref().unwrap();
            primitive::nested_array_to_page::<u16, i32>(array, options, type_, nested)
        }
        UInt32 => {
            let array = array.as_any().downcast_ref().unwrap();
            primitive::nested_array_to_page::<u32, i32>(array, options, type_, nested)
        }
        UInt64 => {
            let array = array.as_any().downcast_ref().unwrap();
            primitive::nested_array_to_page::<u64, i64>(array, options, type_, nested)
        }
        Int8 => {
            let array = array.as_any().downcast_ref().unwrap();
            primitive::nested_array_to_page::<i8, i32>(array, options, type_, nested)
        }
        Int16 => {
            let array = array.as_any().downcast_ref().unwrap();
            primitive::nested_array_to_page::<i16, i32>(array, options, type_, nested)
        }
        Int32 | Date32 | Time32(_) => {
            let array = array.as_any().downcast_ref().unwrap();
            primitive::nested_array_to_page::<i32, i32>(array, options, type_, nested)
        }
        Int64 | Date64 | Time64(_) | Timestamp(_, _) | Duration(_) => {
            let array = array.as_any().downcast_ref().unwrap();
            primitive::nested_array_to_page::<i64, i64>(array, options, type_, nested)
        }
        Float32 => {
            let array = array.as_any().downcast_ref().unwrap();
            primitive::nested_array_to_page::<f32, f32>(array, options, type_, nested)
        }
        Float64 => {
            let array = array.as_any().downcast_ref().unwrap();
            primitive::nested_array_to_page::<f64, f64>(array, options, type_, nested)
        }
        other => Err(Error::NotYetImplemented(format!(
            "Writing nested parquet pages for data type {:?}",
            other
        ))),
    }
    .map(EncodedPage::Data)
}

fn transverse_recursive<T, F: Fn(&DataType) -> T + Clone>(
    data_type: &DataType,
    map: F,
    encodings: &mut Vec<T>,
) {
    use crate::datatypes::PhysicalType::*;
    match data_type.to_physical_type() {
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | Dictionary(_) | LargeUtf8 => encodings.push(map(data_type)),
        List | FixedSizeList | LargeList => {
            let a = data_type.to_logical_type();
            if let DataType::List(inner) = a {
                transverse_recursive(&inner.data_type, map, encodings)
            } else if let DataType::LargeList(inner) = a {
                transverse_recursive(&inner.data_type, map, encodings)
            } else if let DataType::FixedSizeList(inner, _) = a {
                transverse_recursive(&inner.data_type, map, encodings)
            } else {
                unreachable!()
            }
        }
        Struct => {
            if let DataType::Struct(fields) = data_type.to_logical_type() {
                for field in fields {
                    transverse_recursive(&field.data_type, map.clone(), encodings)
                }
            } else {
                unreachable!()
            }
        }
        Union => todo!(),
        Map => todo!(),
    }
}

/// Transverses the `data_type` up to its (parquet) columns and returns a vector of
/// items based on `map`.
/// This is used to assign an [`Encoding`] to every parquet column based on the columns' type (see example)
/// # Example
/// ```
/// use arrow2::io::parquet::write::{transverse, Encoding};
/// use arrow2::datatypes::{DataType, Field};
///
/// let dt = DataType::Struct(vec![
///     Field::new("a", DataType::Int64, true),
///     Field::new("b", DataType::List(Box::new(Field::new("item", DataType::Int32, true))), true),
/// ]);
///
/// let encodings = transverse(&dt, |dt| Encoding::Plain);
/// assert_eq!(encodings, vec![Encoding::Plain, Encoding::Plain]);
/// ```
pub fn transverse<T, F: Fn(&DataType) -> T + Clone>(data_type: &DataType, map: F) -> Vec<T> {
    let mut encodings = vec![];
    transverse_recursive(data_type, map, &mut encodings);
    encodings
}
