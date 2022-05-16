use chrono::{NaiveDate, NaiveDateTime};
use lexical_core::ToLexical;
use std::io::Write;
use streaming_iterator::StreamingIterator;

use crate::bitmap::utils::zip_validity;
use crate::datatypes::TimeUnit;
use crate::io::iterator::BufStreamingIterator;
use crate::temporal_conversions::{
    date32_to_date, date64_to_date, timestamp_ms_to_datetime, timestamp_ns_to_datetime,
    timestamp_s_to_datetime, timestamp_us_to_datetime,
};
use crate::util::lexical_to_bytes_mut;
use crate::{array::*, datatypes::DataType, types::NativeType};

fn boolean_serializer<'a>(
    array: &'a BooleanArray,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync> {
    Box::new(BufStreamingIterator::new(
        array.iter(),
        |x, buf| match x {
            Some(true) => buf.extend_from_slice(b"true"),
            Some(false) => buf.extend_from_slice(b"false"),
            None => buf.extend_from_slice(b"null"),
        },
        vec![],
    ))
}

fn primitive_serializer<'a, T: NativeType + ToLexical>(
    array: &'a PrimitiveArray<T>,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync> {
    Box::new(BufStreamingIterator::new(
        array.iter(),
        |x, buf| {
            if let Some(x) = x {
                lexical_to_bytes_mut(*x, buf)
            } else {
                buf.extend(b"null")
            }
        },
        vec![],
    ))
}

fn float_serializer<'a, T>(
    array: &'a PrimitiveArray<T>,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync>
where
    T: num_traits::Float + NativeType + ToLexical,
{
    Box::new(BufStreamingIterator::new(
        array.iter(),
        |x, buf| {
            if let Some(x) = x {
                if T::is_nan(*x) {
                    buf.extend(b"null")
                } else {
                    lexical_to_bytes_mut(*x, buf)
                }
            } else {
                buf.extend(b"null")
            }
        },
        vec![],
    ))
}

fn utf8_serializer<'a, O: Offset>(
    array: &'a Utf8Array<O>,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync> {
    Box::new(BufStreamingIterator::new(
        array.iter(),
        |x, buf| {
            if let Some(x) = x {
                serde_json::to_writer(buf, x).unwrap();
            } else {
                buf.extend_from_slice(b"null")
            }
        },
        vec![],
    ))
}

fn struct_serializer<'a>(
    array: &'a StructArray,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync> {
    // {"a": [1, 2, 3], "b": [a, b, c], "c": {"a": [1, 2, 3]}}
    // [
    //  {"a": 1, "b": a, "c": {"a": 1}},
    //  {"a": 2, "b": b, "c": {"a": 2}},
    //  {"a": 3, "b": c, "c": {"a": 3}},
    // ]
    //
    let mut serializers = array
        .values()
        .iter()
        .map(|x| x.as_ref())
        .map(new_serializer)
        .collect::<Vec<_>>();
    let names = array.fields().iter().map(|f| f.name.as_str());

    Box::new(BufStreamingIterator::new(
        zip_validity(0..array.len(), array.validity().map(|x| x.iter())),
        move |maybe, buf| {
            if maybe.is_some() {
                let names = names.clone();
                let mut record: Vec<(&str, &[u8])> = Default::default();
                serializers
                    .iter_mut()
                    .zip(names)
                    // `unwrap` is infalible because `array.len()` equals `len` on `Chunk`
                    .for_each(|(iter, name)| {
                        let item = iter.next().unwrap();
                        record.push((name, item));
                    });
                serialize_item(buf, &record, true);
            } else {
                serializers.iter_mut().for_each(|iter| {
                    let _ = iter.next();
                });
                buf.extend(b"null");
            }
        },
        vec![],
    ))
}

fn list_serializer<'a, O: Offset>(
    array: &'a ListArray<O>,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync> {
    // [[1, 2], [3]]
    // [
    //  [1, 2],
    //  [3]
    // ]
    //
    let mut serializer = new_serializer(array.values().as_ref());

    Box::new(BufStreamingIterator::new(
        zip_validity(
            array.offsets().windows(2),
            array.validity().map(|x| x.iter()),
        ),
        move |offset, buf| {
            if let Some(offset) = offset {
                let length = (offset[1] - offset[0]).to_usize();
                buf.push(b'[');
                let mut is_first_row = true;
                for _ in 0..length {
                    if !is_first_row {
                        buf.push(b',');
                    }
                    is_first_row = false;
                    buf.extend(serializer.next().unwrap());
                }
                buf.push(b']');
            } else {
                buf.extend(b"null");
            }
        },
        vec![],
    ))
}

fn date_serializer<'a, T, F>(
    array: &'a PrimitiveArray<T>,
    convert: F,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync>
where
    T: NativeType,
    F: Fn(T) -> NaiveDate + 'static + Send + Sync,
{
    Box::new(BufStreamingIterator::new(
        array.iter(),
        move |x, buf| {
            if let Some(x) = x {
                let nd = convert(*x);
                write!(buf, "\"{}\"", nd).unwrap();
            } else {
                buf.extend_from_slice(b"null")
            }
        },
        vec![],
    ))
}

fn timestamp_serializer<'a, F>(
    array: &'a PrimitiveArray<i64>,
    convert: F,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync>
where
    F: Fn(i64) -> NaiveDateTime + 'static + Send + Sync,
{
    Box::new(BufStreamingIterator::new(
        array.iter(),
        move |x, buf| {
            if let Some(x) = x {
                let ndt = convert(*x);
                write!(buf, "\"{}\"", ndt).unwrap();
            } else {
                buf.extend_from_slice(b"null")
            }
        },
        vec![],
    ))
}

pub(crate) fn new_serializer<'a>(
    array: &'a dyn Array,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync> {
    match array.data_type().to_logical_type() {
        DataType::Boolean => boolean_serializer(array.as_any().downcast_ref().unwrap()),
        DataType::Int8 => primitive_serializer::<i8>(array.as_any().downcast_ref().unwrap()),
        DataType::Int16 => primitive_serializer::<i16>(array.as_any().downcast_ref().unwrap()),
        DataType::Int32 => primitive_serializer::<i32>(array.as_any().downcast_ref().unwrap()),
        DataType::Int64 => primitive_serializer::<i64>(array.as_any().downcast_ref().unwrap()),
        DataType::UInt8 => primitive_serializer::<u8>(array.as_any().downcast_ref().unwrap()),
        DataType::UInt16 => primitive_serializer::<u16>(array.as_any().downcast_ref().unwrap()),
        DataType::UInt32 => primitive_serializer::<u32>(array.as_any().downcast_ref().unwrap()),
        DataType::UInt64 => primitive_serializer::<u64>(array.as_any().downcast_ref().unwrap()),
        DataType::Float32 => float_serializer::<f32>(array.as_any().downcast_ref().unwrap()),
        DataType::Float64 => float_serializer::<f64>(array.as_any().downcast_ref().unwrap()),
        DataType::Utf8 => utf8_serializer::<i32>(array.as_any().downcast_ref().unwrap()),
        DataType::LargeUtf8 => utf8_serializer::<i64>(array.as_any().downcast_ref().unwrap()),
        DataType::Struct(_) => struct_serializer(array.as_any().downcast_ref().unwrap()),
        DataType::List(_) => list_serializer::<i32>(array.as_any().downcast_ref().unwrap()),
        DataType::LargeList(_) => list_serializer::<i64>(array.as_any().downcast_ref().unwrap()),
        DataType::Date32 => date_serializer(array.as_any().downcast_ref().unwrap(), date32_to_date),
        DataType::Date64 => date_serializer(array.as_any().downcast_ref().unwrap(), date64_to_date),
        DataType::Timestamp(tu, tz) => {
            if tz.is_some() {
                todo!("still have to implement timezone")
            } else {
                let convert = match tu {
                    TimeUnit::Nanosecond => timestamp_ns_to_datetime,
                    TimeUnit::Microsecond => timestamp_us_to_datetime,
                    TimeUnit::Millisecond => timestamp_ms_to_datetime,
                    TimeUnit::Second => timestamp_s_to_datetime,
                };
                timestamp_serializer(array.as_any().downcast_ref().unwrap(), convert)
            }
        }
        other => todo!("Writing {:?} to JSON", other),
    }
}

fn serialize_item(buffer: &mut Vec<u8>, record: &[(&str, &[u8])], is_first_row: bool) {
    if !is_first_row {
        buffer.push(b',');
    }
    buffer.push(b'{');
    let mut first_item = true;
    for (key, value) in record {
        if !first_item {
            buffer.push(b',');
        }
        first_item = false;
        serde_json::to_writer(&mut *buffer, key).unwrap();
        buffer.push(b':');
        buffer.extend(*value);
    }
    buffer.push(b'}');
}

/// Serializes `array` to a valid JSON to `buffer`
/// # Implementation
/// This operation is CPU-bounded
pub(crate) fn serialize(array: &dyn Array, buffer: &mut Vec<u8>) {
    let mut serializer = new_serializer(array);

    (0..array.len()).for_each(|i| {
        if i != 0 {
            buffer.push(b',');
        }
        buffer.extend_from_slice(serializer.next().unwrap());
    });
}
