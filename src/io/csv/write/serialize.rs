use lexical_core::ToLexical;

use crate::temporal_conversions;
use crate::types::NativeType;
use crate::util::lexical_to_bytes_mut;
use crate::{
    array::{Array, BinaryArray, BooleanArray, PrimitiveArray, Utf8Array},
    datatypes::{DataType, TimeUnit},
    error::Result,
};

use super::iterator::{BufStreamingIterator, StreamingIterator};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct SerializeOptions {
    // used for date32
    pub date_format: String,
    /// used for dat64
    pub datetime_format: String,
    // used for time32/64
    pub time_format: String,
    // used for timestamp
    pub timestamp_format: String,
}

impl Default for SerializeOptions {
    fn default() -> Self {
        Self {
            date_format: "%F".to_string(),
            datetime_format: "%FT%H:%M:%S.%9f".to_string(),
            time_format: "%T".to_string(),
            timestamp_format: "%FT%H:%M:%S.%9f".to_string(),
        }
    }
}

fn primitive_write<'a, T: NativeType + ToLexical>(
    array: &'a PrimitiveArray<T>,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a> {
    Box::new(BufStreamingIterator::new(
        array.iter(),
        |x, buf| {
            if let Some(x) = x {
                lexical_to_bytes_mut(*x, buf)
            }
        },
        vec![],
    ))
}

macro_rules! dyn_primitive {
    ($ty:ty, $array:expr) => {{
        let array = $array.as_any().downcast_ref().unwrap();
        primitive_write::<$ty>(array)
    }};
}

macro_rules! dyn_date {
    ($ty:ident, $fn:expr, $array:expr, $format:expr) => {{
        let array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$ty>>()
            .unwrap();
        Box::new(BufStreamingIterator::new(
            array.iter(),
            move |x, buf| {
                if let Some(x) = x {
                    buf.extend_from_slice(($fn)(*x).format($format).to_string().as_bytes())
                }
            },
            vec![],
        ))
    }};
}

/// Returns an Iterator that returns items of `Array` as `Vec<u8>`, according to `options`.
/// For numeric types, this serializes as usual. For dates, times and timestamps, it uses `options` to
/// Supported types:
/// * boolean
/// * numeric types (i.e. floats, int, uint)
/// * times and dates
/// * naive timestamps (timestamps without timezone information)
/// # Error
/// This function errors if any of the logical types in `batch` is not supported.
pub fn new_serializer<'a>(
    array: &'a dyn Array,
    options: &'a SerializeOptions,
) -> Result<Box<dyn StreamingIterator<Item = [u8]> + 'a>> {
    Ok(match array.data_type() {
        DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Box::new(BufStreamingIterator::new(
                array.iter(),
                |x, buf| {
                    if let Some(x) = x {
                        if x {
                            buf.extend_from_slice(b"true");
                        } else {
                            buf.extend_from_slice(b"false");
                        }
                    }
                },
                vec![],
            ))
        }
        DataType::UInt8 => {
            dyn_primitive!(u8, array)
        }
        DataType::UInt16 => {
            dyn_primitive!(u16, array)
        }
        DataType::UInt32 => {
            dyn_primitive!(u32, array)
        }
        DataType::UInt64 => {
            dyn_primitive!(u64, array)
        }
        DataType::Int8 => {
            dyn_primitive!(i8, array)
        }
        DataType::Int16 => {
            dyn_primitive!(i16, array)
        }
        DataType::Int32 => {
            dyn_primitive!(i32, array)
        }
        DataType::Date32 => {
            dyn_date!(
                i32,
                temporal_conversions::date32_to_datetime,
                array,
                &options.date_format
            )
        }
        DataType::Time32(TimeUnit::Second) => {
            dyn_date!(
                i32,
                temporal_conversions::time32s_to_time,
                array,
                &options.time_format
            )
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            dyn_date!(
                i32,
                temporal_conversions::time32ms_to_time,
                array,
                &options.time_format
            )
        }
        DataType::Int64 => {
            dyn_primitive!(i64, array)
        }
        DataType::Date64 => {
            dyn_date!(
                i64,
                temporal_conversions::date64_to_datetime,
                array,
                &options.datetime_format
            )
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            dyn_date!(
                i64,
                temporal_conversions::time64us_to_time,
                array,
                &options.time_format
            )
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            dyn_date!(
                i64,
                temporal_conversions::time64ns_to_time,
                array,
                &options.time_format
            )
        }
        DataType::Timestamp(TimeUnit::Second, None) => {
            dyn_date!(
                i64,
                temporal_conversions::timestamp_s_to_datetime,
                array,
                &options.timestamp_format
            )
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            dyn_date!(
                i64,
                temporal_conversions::timestamp_ms_to_datetime,
                array,
                &options.timestamp_format
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            dyn_date!(
                i64,
                temporal_conversions::timestamp_us_to_datetime,
                array,
                &options.timestamp_format
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            dyn_date!(
                i64,
                temporal_conversions::timestamp_ns_to_datetime,
                array,
                &options.timestamp_format
            )
        }
        DataType::Float32 => {
            dyn_primitive!(f32, array)
        }
        DataType::Float64 => {
            dyn_primitive!(f64, array)
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            Box::new(BufStreamingIterator::new(
                array.iter(),
                |x, buf| {
                    if let Some(x) = x {
                        buf.extend_from_slice(x.as_bytes());
                    }
                },
                vec![],
            ))
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            Box::new(BufStreamingIterator::new(
                array.iter(),
                |x, buf| {
                    if let Some(x) = x {
                        buf.extend_from_slice(x.as_bytes());
                    }
                },
                vec![],
            ))
        }
        DataType::Binary => {
            let array = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            Box::new(BufStreamingIterator::new(
                array.iter(),
                |x, buf| {
                    if let Some(x) = x {
                        buf.extend_from_slice(x);
                    }
                },
                vec![],
            ))
        }
        DataType::LargeBinary => {
            let array = array.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
            Box::new(BufStreamingIterator::new(
                array.iter(),
                |x, buf| {
                    if let Some(x) = x {
                        buf.extend_from_slice(x);
                    }
                },
                vec![],
            ))
        }
        _ => todo!(),
    })
}
