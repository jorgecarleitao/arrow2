use avro_schema::Schema as AvroSchema;

use crate::{array::*, datatypes::DataType};

use super::super::super::iterator::*;
use super::util;

/// A type alias for a boxed [`StreamingIterator`], used to write arrays into avro rows
/// (i.e. a column -> row transposition of types known at run-time)
pub type BoxSerializer<'a> = Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync>;

/// Creates a [`StreamingIterator`] trait object that presents items from `array`
/// encoded according to `schema`.
/// # Panic
/// This function panics iff the `data_type` is not supported (use [`can_serialize`] to check)
/// # Implementation
/// This function performs minimal CPU work: it dynamically dispatches based on the schema
/// and arrow type.
pub fn new_serializer<'a>(array: &'a dyn Array, schema: &AvroSchema) -> BoxSerializer<'a> {
    let data_type = array.data_type().to_logical_type();

    match (data_type, schema) {
        (DataType::Boolean, AvroSchema::Boolean) => {
            let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Box::new(BufStreamingIterator::new(
                values.values_iter(),
                |x, buf| {
                    buf.push(x as u8);
                },
                vec![],
            ))
        }
        (DataType::Boolean, AvroSchema::Union(_)) => {
            let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Box::new(BufStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    util::zigzag_encode(x.is_some() as i64, buf).unwrap();
                    if let Some(x) = x {
                        buf.push(x as u8);
                    }
                },
                vec![],
            ))
        }
        (DataType::Utf8, AvroSchema::Union(_)) => {
            let values = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            Box::new(BufStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    util::zigzag_encode(x.is_some() as i64, buf).unwrap();
                    if let Some(x) = x {
                        util::zigzag_encode(x.len() as i64, buf).unwrap();
                        buf.extend_from_slice(x.as_bytes());
                    }
                },
                vec![],
            ))
        }
        (DataType::Utf8, AvroSchema::String(_)) => {
            let values = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            Box::new(BufStreamingIterator::new(
                values.values_iter(),
                |x, buf| {
                    util::zigzag_encode(x.len() as i64, buf).unwrap();
                    buf.extend_from_slice(x.as_bytes());
                },
                vec![],
            ))
        }
        (DataType::Binary, AvroSchema::Union(_)) => {
            let values = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            Box::new(BufStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    util::zigzag_encode(x.is_some() as i64, buf).unwrap();
                    if let Some(x) = x {
                        util::zigzag_encode(x.len() as i64, buf).unwrap();
                        buf.extend_from_slice(x);
                    }
                },
                vec![],
            ))
        }
        (DataType::Binary, AvroSchema::Bytes(_)) => {
            let values = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            Box::new(BufStreamingIterator::new(
                values.values_iter(),
                |x, buf| {
                    util::zigzag_encode(x.len() as i64, buf).unwrap();
                    buf.extend_from_slice(x);
                },
                vec![],
            ))
        }

        (DataType::Int32, AvroSchema::Union(_)) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            Box::new(BufStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    util::zigzag_encode(x.is_some() as i64, buf).unwrap();
                    if let Some(x) = x {
                        util::zigzag_encode(*x as i64, buf).unwrap();
                    }
                },
                vec![],
            ))
        }
        (DataType::Int32, AvroSchema::Int(_)) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            Box::new(BufStreamingIterator::new(
                values.values().iter(),
                |x, buf| {
                    util::zigzag_encode(*x as i64, buf).unwrap();
                },
                vec![],
            ))
        }
        (DataType::Int64, AvroSchema::Union(_)) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            Box::new(BufStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    util::zigzag_encode(x.is_some() as i64, buf).unwrap();
                    if let Some(x) = x {
                        util::zigzag_encode(*x, buf).unwrap();
                    }
                },
                vec![],
            ))
        }
        (DataType::Int64, AvroSchema::Long(_)) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            Box::new(BufStreamingIterator::new(
                values.values().iter(),
                |x, buf| {
                    util::zigzag_encode(*x, buf).unwrap();
                },
                vec![],
            ))
        }
        _ => todo!(),
    }
}

/// Whether [`new_serializer`] supports `data_type`.
pub fn can_serialize(data_type: &DataType) -> bool {
    use DataType::*;
    matches!(data_type, Boolean | Int32 | Int64 | Utf8 | Binary)
}
