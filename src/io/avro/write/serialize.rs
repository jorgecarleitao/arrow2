use avro_schema::Schema as AvroSchema;

use crate::datatypes::{IntervalUnit, PhysicalType, PrimitiveType};
use crate::types::months_days_ns;
use crate::{array::*, datatypes::DataType};

use super::super::super::iterator::*;
use super::util;

/// A type alias for a boxed [`StreamingIterator`], used to write arrays into avro rows
/// (i.e. a column -> row transposition of types known at run-time)
pub type BoxSerializer<'a> = Box<dyn StreamingIterator<Item = [u8]> + 'a + Send + Sync>;

fn utf8_required<O: Offset>(array: &Utf8Array<O>) -> BoxSerializer {
    Box::new(BufStreamingIterator::new(
        array.values_iter(),
        |x, buf| {
            util::zigzag_encode(x.len() as i64, buf).unwrap();
            buf.extend_from_slice(x.as_bytes());
        },
        vec![],
    ))
}

fn utf8_optional<O: Offset>(array: &Utf8Array<O>) -> BoxSerializer {
    Box::new(BufStreamingIterator::new(
        array.iter(),
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

fn binary_required<O: Offset>(array: &BinaryArray<O>) -> BoxSerializer {
    Box::new(BufStreamingIterator::new(
        array.values_iter(),
        |x, buf| {
            util::zigzag_encode(x.len() as i64, buf).unwrap();
            buf.extend_from_slice(x);
        },
        vec![],
    ))
}

fn binary_optional<O: Offset>(array: &BinaryArray<O>) -> BoxSerializer {
    Box::new(BufStreamingIterator::new(
        array.iter(),
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

/// Creates a [`StreamingIterator`] trait object that presents items from `array`
/// encoded according to `schema`.
/// # Panic
/// This function panics iff the `data_type` is not supported (use [`can_serialize`] to check)
/// # Implementation
/// This function performs minimal CPU work: it dynamically dispatches based on the schema
/// and arrow type.
pub fn new_serializer<'a>(array: &'a dyn Array, schema: &AvroSchema) -> BoxSerializer<'a> {
    let data_type = array.data_type().to_physical_type();

    match (data_type, schema) {
        (PhysicalType::Boolean, AvroSchema::Boolean) => {
            let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Box::new(BufStreamingIterator::new(
                values.values_iter(),
                |x, buf| {
                    buf.push(x as u8);
                },
                vec![],
            ))
        }
        (PhysicalType::Boolean, AvroSchema::Union(_)) => {
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
        (PhysicalType::Utf8, AvroSchema::Union(_)) => {
            utf8_optional::<i32>(array.as_any().downcast_ref().unwrap())
        }
        (PhysicalType::LargeUtf8, AvroSchema::Union(_)) => {
            utf8_optional::<i64>(array.as_any().downcast_ref().unwrap())
        }
        (PhysicalType::Utf8, AvroSchema::String(_)) => {
            utf8_required::<i32>(array.as_any().downcast_ref().unwrap())
        }
        (PhysicalType::LargeUtf8, AvroSchema::String(_)) => {
            utf8_required::<i64>(array.as_any().downcast_ref().unwrap())
        }
        (PhysicalType::Binary, AvroSchema::Union(_)) => {
            binary_optional::<i32>(array.as_any().downcast_ref().unwrap())
        }
        (PhysicalType::LargeBinary, AvroSchema::Union(_)) => {
            binary_optional::<i64>(array.as_any().downcast_ref().unwrap())
        }
        (PhysicalType::Binary, AvroSchema::Bytes(_)) => {
            binary_required::<i32>(array.as_any().downcast_ref().unwrap())
        }
        (PhysicalType::LargeBinary, AvroSchema::Bytes(_)) => {
            binary_required::<i64>(array.as_any().downcast_ref().unwrap())
        }

        (PhysicalType::Primitive(PrimitiveType::Int32), AvroSchema::Union(_)) => {
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
        (PhysicalType::Primitive(PrimitiveType::Int32), AvroSchema::Int(_)) => {
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
        (PhysicalType::Primitive(PrimitiveType::Int64), AvroSchema::Union(_)) => {
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
        (PhysicalType::Primitive(PrimitiveType::Int64), AvroSchema::Long(_)) => {
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
        (PhysicalType::Primitive(PrimitiveType::Float32), AvroSchema::Union(_)) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<f32>>()
                .unwrap();
            Box::new(BufStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    util::zigzag_encode(x.is_some() as i64, buf).unwrap();
                    if let Some(x) = x {
                        buf.extend(x.to_le_bytes())
                    }
                },
                vec![],
            ))
        }
        (PhysicalType::Primitive(PrimitiveType::Float32), AvroSchema::Float) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<f32>>()
                .unwrap();
            Box::new(BufStreamingIterator::new(
                values.values().iter(),
                |x, buf| {
                    buf.extend_from_slice(&x.to_le_bytes());
                },
                vec![],
            ))
        }
        (PhysicalType::Primitive(PrimitiveType::Float64), AvroSchema::Union(_)) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<f64>>()
                .unwrap();
            Box::new(BufStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    util::zigzag_encode(x.is_some() as i64, buf).unwrap();
                    if let Some(x) = x {
                        buf.extend(x.to_le_bytes())
                    }
                },
                vec![],
            ))
        }
        (PhysicalType::Primitive(PrimitiveType::Float64), AvroSchema::Double) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<f64>>()
                .unwrap();
            Box::new(BufStreamingIterator::new(
                values.values().iter(),
                |x, buf| {
                    buf.extend_from_slice(&x.to_le_bytes());
                },
                vec![],
            ))
        }
        (PhysicalType::Primitive(PrimitiveType::MonthDayNano), AvroSchema::Fixed(_)) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<months_days_ns>>()
                .unwrap();
            Box::new(BufStreamingIterator::new(
                values.values().iter(),
                interval_write,
                vec![],
            ))
        }
        (PhysicalType::Primitive(PrimitiveType::MonthDayNano), AvroSchema::Union(_)) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<months_days_ns>>()
                .unwrap();
            Box::new(BufStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    util::zigzag_encode(x.is_some() as i64, buf).unwrap();
                    if let Some(x) = x {
                        interval_write(x, buf)
                    }
                },
                vec![],
            ))
        }
        (a, b) => todo!("{:?} -> {:?} not supported", a, b),
    }
}

/// Whether [`new_serializer`] supports `data_type`.
pub fn can_serialize(data_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        data_type,
        Boolean | Int32 | Int64 | Utf8 | Binary | Interval(IntervalUnit::MonthDayNano)
    )
}

#[inline]
fn interval_write(x: &months_days_ns, buf: &mut Vec<u8>) {
    // https://avro.apache.org/docs/current/spec.html#Duration
    // 12 bytes, months, days, millis in LE
    buf.reserve(12);
    buf.extend(x.months().to_le_bytes());
    buf.extend(x.days().to_le_bytes());
    buf.extend(((x.ns() / 1_000_000) as i32).to_le_bytes());
}
