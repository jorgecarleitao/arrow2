//! APIs to write to Parquet format.
mod binary;
mod boolean;
mod dictionary;
mod file;
mod fixed_len_bytes;
mod levels;
mod primitive;
mod row_group;
mod schema;
mod sink;
mod utf8;
mod utils;

use crate::array::*;
use crate::bitmap::Bitmap;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::io::parquet::read::schema::is_nullable;
use crate::io::parquet::write::levels::NestedInfo;
use crate::types::days_ms;
use crate::types::NativeType;

use parquet2::page::DataPage;
pub use parquet2::{
    compression::CompressionOptions,
    encoding::Encoding,
    fallible_streaming_iterator,
    metadata::{Descriptor, KeyValue, SchemaDescriptor},
    page::{CompressedDataPage, CompressedPage, EncodedPage},
    schema::types::ParquetType,
    write::{compress, Compressor, DynIter, DynStreamingIterator, RowGroupIter, Version},
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
    descriptor: Descriptor,
    options: WriteOptions,
    encoding: Encoding,
) -> Result<DynIter<'static, Result<EncodedPage>>> {
    // maximum page size is 2^31 e.g. i32::MAX
    // we split at 2^30 to err on the safe side
    if estimated_bytes_size(array) >= 2u32.pow(30) as usize {
        let split_at = array.len() / 2;
        let left = array.slice(0, split_at);
        let right = array.slice(split_at, array.len() - split_at);

        Ok(DynIter::new(
            array_to_pages(&*left, descriptor.clone(), options, encoding)?
                .chain(array_to_pages(&*right, descriptor, options, encoding)?),
        ))
    } else {
        match array.data_type() {
            DataType::Dictionary(key_type, _, _) => {
                match_integer_type!(key_type, |$T| {
                    dictionary::array_to_pages::<$T>(
                        array.as_any().downcast_ref().unwrap(),
                        descriptor,
                        options,
                        encoding,
                    )
                })
            }
            _ => array_to_page(array, descriptor, options, encoding)
                .map(|page| DynIter::new(std::iter::once(Ok(page)))),
        }
    }
}

/// Converts an [`Array`] to a [`CompressedPage`] based on options, descriptor and `encoding`.
pub fn array_to_page(
    array: &dyn Array,
    descriptor: Descriptor,
    options: WriteOptions,
    encoding: Encoding,
) -> Result<EncodedPage> {
    let data_type = array.data_type();
    if !can_encode(data_type, encoding) {
        return Err(ArrowError::InvalidArgumentError(format!(
            "The datatype {:?} cannot be encoded by {:?}",
            data_type, encoding
        )));
    }

    match data_type.to_logical_type() {
        DataType::Boolean => {
            boolean::array_to_page(array.as_any().downcast_ref().unwrap(), options, descriptor)
        }
        // casts below MUST match the casts done at the metadata (field -> parquet type).
        DataType::UInt8 => primitive::array_to_page::<u8, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::UInt16 => primitive::array_to_page::<u16, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::UInt32 => primitive::array_to_page::<u32, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::UInt64 => primitive::array_to_page::<u64, i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Int8 => primitive::array_to_page::<i8, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Int16 => primitive::array_to_page::<i16, i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            primitive::array_to_page::<i32, i32>(
                array.as_any().downcast_ref().unwrap(),
                options,
                descriptor,
            )
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => primitive::array_to_page::<i64, i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Float32 => primitive::array_to_page::<f32, f32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Float64 => primitive::array_to_page::<f64, f64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
        ),
        DataType::Utf8 => utf8::array_to_page::<i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
            encoding,
        ),
        DataType::LargeUtf8 => utf8::array_to_page::<i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
            encoding,
        ),
        DataType::Binary => binary::array_to_page::<i32>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
            encoding,
        ),
        DataType::LargeBinary => binary::array_to_page::<i64>(
            array.as_any().downcast_ref().unwrap(),
            options,
            descriptor,
            encoding,
        ),
        DataType::Null => {
            let array = Int32Array::new_null(DataType::Int32, array.len());
            primitive::array_to_page::<i32, i32>(&array, options, descriptor)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            let mut values = Vec::<u8>::with_capacity(12 * array.len());
            array.values().iter().for_each(|x| {
                let bytes = &x.to_le_bytes();
                values.extend_from_slice(bytes);
                values.resize(values.len() + 8, 0);
            });
            let array = FixedSizeBinaryArray::new(
                DataType::FixedSizeBinary(12),
                values.into(),
                array.validity().cloned(),
            );
            let statistics = if options.write_statistics {
                Some(fixed_len_bytes::build_statistics(
                    &array,
                    descriptor.primitive_type.clone(),
                ))
            } else {
                None
            };
            fixed_len_bytes::array_to_page(&array, options, descriptor, statistics)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<days_ms>>()
                .unwrap();
            let mut values = Vec::<u8>::with_capacity(12 * array.len());
            array.values().iter().for_each(|x| {
                let bytes = &x.to_le_bytes();
                values.resize(values.len() + 4, 0); // months
                values.extend_from_slice(bytes); // days and seconds
            });
            let array = FixedSizeBinaryArray::from_data(
                DataType::FixedSizeBinary(12),
                values.into(),
                array.validity().cloned(),
            );
            let statistics = if options.write_statistics {
                Some(fixed_len_bytes::build_statistics(
                    &array,
                    descriptor.primitive_type.clone(),
                ))
            } else {
                None
            };
            fixed_len_bytes::array_to_page(&array, options, descriptor, statistics)
        }
        DataType::FixedSizeBinary(_) => {
            let array = array.as_any().downcast_ref().unwrap();
            let statistics = if options.write_statistics {
                Some(fixed_len_bytes::build_statistics(
                    array,
                    descriptor.primitive_type.clone(),
                ))
            } else {
                None
            };

            fixed_len_bytes::array_to_page(array, options, descriptor, statistics)
        }
        DataType::Decimal(precision, _) => {
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
                primitive::array_to_page::<i32, i32>(&array, options, descriptor)
            } else if precision <= 18 {
                let values = array
                    .values()
                    .iter()
                    .map(|x| *x as i64)
                    .collect::<Vec<_>>()
                    .into();

                let array =
                    PrimitiveArray::<i64>::new(DataType::Int64, values, array.validity().cloned());
                primitive::array_to_page::<i64, i64>(&array, options, descriptor)
            } else {
                let size = decimal_length_from_precision(precision);

                let statistics = if options.write_statistics {
                    let stats = fixed_len_bytes::build_statistics_decimal(
                        array,
                        descriptor.primitive_type.clone(),
                        size,
                    );
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
                fixed_len_bytes::array_to_page(&array, options, descriptor, statistics)
            }
        }
        DataType::FixedSizeList(_, _) | DataType::List(_) | DataType::LargeList(_) => {
            nested_array_to_page(array, descriptor, options)
        }
        other => Err(ArrowError::NotYetImplemented(format!(
            "Writing parquet V1 pages for data type {:?}",
            other
        ))),
    }
    .map(EncodedPage::Data)
}

macro_rules! dyn_nested_prim {
    ($from:ty, $to:ty, $offset:ty, $values:expr, $nested:expr,$descriptor:expr, $options:expr) => {{
        let values = $values.as_any().downcast_ref().unwrap();

        primitive::nested_array_to_page::<$from, $to, $offset>(
            values,
            $options,
            $descriptor,
            $nested,
        )
    }};
}

fn list_array_to_page<O: Offset>(
    offsets: &[O],
    validity: Option<&Bitmap>,
    values: &dyn Array,
    descriptor: Descriptor,
    options: WriteOptions,
) -> Result<DataPage> {
    use DataType::*;
    let is_optional = is_nullable(&descriptor.primitive_type.field_info);
    let nested = NestedInfo::new(offsets, validity, is_optional);

    match values.data_type() {
        Boolean => {
            let values = values.as_any().downcast_ref().unwrap();
            boolean::nested_array_to_page::<O>(values, options, descriptor, nested)
        }
        UInt8 => dyn_nested_prim!(u8, i32, O, values, nested, descriptor, options),
        UInt16 => dyn_nested_prim!(u16, i32, O, values, nested, descriptor, options),
        UInt32 => dyn_nested_prim!(u32, i32, O, values, nested, descriptor, options),
        UInt64 => dyn_nested_prim!(u64, i64, O, values, nested, descriptor, options),

        Int8 => dyn_nested_prim!(i8, i32, O, values, nested, descriptor, options),
        Int16 => dyn_nested_prim!(i16, i32, O, values, nested, descriptor, options),
        Int32 | Date32 | Time32(_) => {
            dyn_nested_prim!(i32, i32, O, values, nested, descriptor, options)
        }
        Int64 | Date64 | Time64(_) | Timestamp(_, _) | Duration(_) => {
            dyn_nested_prim!(i64, i64, O, values, nested, descriptor, options)
        }

        Float32 => dyn_nested_prim!(f32, f32, O, values, nested, descriptor, options),
        Float64 => dyn_nested_prim!(f64, f64, O, values, nested, descriptor, options),

        Utf8 => {
            let values = values.as_any().downcast_ref().unwrap();
            utf8::nested_array_to_page::<i32, O>(values, options, descriptor, nested)
        }
        LargeUtf8 => {
            let values = values.as_any().downcast_ref().unwrap();
            utf8::nested_array_to_page::<i64, O>(values, options, descriptor, nested)
        }
        Binary => {
            let values = values.as_any().downcast_ref().unwrap();
            binary::nested_array_to_page::<i32, O>(values, options, descriptor, nested)
        }
        LargeBinary => {
            let values = values.as_any().downcast_ref().unwrap();
            binary::nested_array_to_page::<i64, O>(values, options, descriptor, nested)
        }
        _ => todo!(),
    }
}

fn nested_array_to_page(
    array: &dyn Array,
    descriptor: Descriptor,
    options: WriteOptions,
) -> Result<DataPage> {
    match array.data_type() {
        DataType::List(_) => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            list_array_to_page(
                array.offsets(),
                array.validity(),
                array.values().as_ref(),
                descriptor,
                options,
            )
        }
        DataType::LargeList(_) => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            list_array_to_page(
                array.offsets(),
                array.validity(),
                array.values().as_ref(),
                descriptor,
                options,
            )
        }
        DataType::FixedSizeList(_, size) => {
            let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let offsets = (0..=array.len())
                .map(|x| (*size * x) as i32)
                .collect::<Vec<_>>();
            list_array_to_page(
                &offsets,
                array.validity(),
                array.values().as_ref(),
                descriptor,
                options,
            )
        }
        _ => todo!(),
    }
}
