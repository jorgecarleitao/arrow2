use lexical_core::ToLexical;
use serde_json::Value;
use streaming_iterator::StreamingIterator;

use crate::bitmap::utils::zip_validity;
use crate::chunk::Chunk;
use crate::io::iterator::BufStreamingIterator;
use crate::util::lexical_to_bytes_mut;
use crate::{array::*, datatypes::DataType, types::NativeType};

use super::{JsonArray, JsonFormat};

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

fn utf8_serializer<'a, O: Offset>(
    array: &'a Utf8Array<O>,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync> {
    Box::new(BufStreamingIterator::new(
        array.iter(),
        |x, buf| {
            if let Some(x) = x {
                utf8_serialize(x, buf)
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
    let names = array.fields().iter().map(|f| f.name().as_str());

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
                serialize_item(buf, &record, JsonArray::default(), true);
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

#[inline]
fn utf8_serialize(value: &str, buf: &mut Vec<u8>) {
    if value.as_bytes().is_ascii() {
        buf.reserve(value.len() + 2);
        buf.push(b'"');
        buf.extend_from_slice(value.as_bytes());
        buf.push(b'"');
    } else {
        // it may contain reserved keywords: perform roundtrip for
        // todo: avoid this roundtrip over serde_json
        serde_json::to_writer(buf, &Value::String(value.to_string())).unwrap();
    }
}

fn new_serializer<'a>(
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
        DataType::Float32 => primitive_serializer::<f32>(array.as_any().downcast_ref().unwrap()),
        DataType::Float64 => primitive_serializer::<f64>(array.as_any().downcast_ref().unwrap()),
        DataType::Utf8 => utf8_serializer::<i32>(array.as_any().downcast_ref().unwrap()),
        DataType::LargeUtf8 => utf8_serializer::<i64>(array.as_any().downcast_ref().unwrap()),
        DataType::Struct(_) => struct_serializer(array.as_any().downcast_ref().unwrap()),
        DataType::List(_) => list_serializer::<i32>(array.as_any().downcast_ref().unwrap()),
        DataType::LargeList(_) => list_serializer::<i64>(array.as_any().downcast_ref().unwrap()),
        other => todo!("Writing {:?} to JSON", other),
    }
}

fn serialize_item<F: JsonFormat>(
    buffer: &mut Vec<u8>,
    record: &[(&str, &[u8])],
    format: F,
    is_first_row: bool,
) {
    format.start_row(buffer, is_first_row).unwrap();
    buffer.push(b'{');
    let mut first_item = true;
    for (key, value) in record {
        if !first_item {
            buffer.push(b',');
        }
        first_item = false;
        utf8_serialize(key, buffer);
        buffer.push(b':');
        buffer.extend(*value);
    }
    buffer.push(b'}');
    format.end_row(buffer).unwrap();
}

/// Serializes a (name, array) to a valid JSON to `buffer`
/// This is CPU-bounded
pub fn serialize<N, A, F>(names: &[N], columns: &Chunk<A>, format: F, buffer: &mut Vec<u8>)
where
    N: AsRef<str>,
    A: AsRef<dyn Array>,
    F: JsonFormat,
{
    let num_rows = columns.len();

    let mut serializers: Vec<_> = columns
        .arrays()
        .iter()
        .map(|array| new_serializer(array.as_ref()))
        .collect();

    let mut is_first_row = true;
    (0..num_rows).for_each(|_| {
        let mut record: Vec<(&str, &[u8])> = Default::default();
        serializers
            .iter_mut()
            .zip(names.iter())
            // `unwrap` is infalible because `array.len()` equals `len` on `Chunk`
            .for_each(|(iter, name)| {
                let item = iter.next().unwrap();
                record.push((name.as_ref(), item));
            });
        serialize_item(buffer, &record, format, is_first_row);
        is_first_row = false;
    })
}
