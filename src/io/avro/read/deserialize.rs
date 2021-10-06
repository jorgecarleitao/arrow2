use std::convert::TryInto;
use std::sync::Arc;

use avro_rs::Schema as AvroSchema;

use crate::array::*;
use crate::datatypes::*;
use crate::error::ArrowError;
use crate::error::Result;
use crate::record_batch::RecordBatch;
use crate::types::months_days_ns;

use super::nested::*;
use super::util;

fn make_mutable(
    data_type: &DataType,
    avro_schema: Option<&AvroSchema>,
    capacity: usize,
) -> Result<Box<dyn MutableArray>> {
    Ok(match data_type.to_physical_type() {
        PhysicalType::Boolean => {
            Box::new(MutableBooleanArray::with_capacity(capacity)) as Box<dyn MutableArray>
        }
        PhysicalType::Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            Box::new(MutablePrimitiveArray::<$T>::with_capacity(capacity).to(data_type.clone()))
                as Box<dyn MutableArray>
        }),
        PhysicalType::Binary => {
            Box::new(MutableBinaryArray::<i32>::with_capacity(capacity)) as Box<dyn MutableArray>
        }
        PhysicalType::Utf8 => {
            Box::new(MutableUtf8Array::<i32>::with_capacity(capacity)) as Box<dyn MutableArray>
        }
        PhysicalType::Dictionary(_) => {
            if let Some(AvroSchema::Enum { symbols, .. }) = avro_schema {
                let values = Utf8Array::<i32>::from_slice(symbols);
                Box::new(FixedItemsUtf8Dictionary::with_capacity(values, capacity))
                    as Box<dyn MutableArray>
            } else {
                unreachable!()
            }
        }
        _ => match data_type {
            DataType::List(inner) => {
                let values = make_mutable(inner.data_type(), None, 0)?;
                Box::new(DynMutableListArray::<i32>::new_with_capacity(
                    values, capacity,
                )) as Box<dyn MutableArray>
            }
            DataType::FixedSizeBinary(size) => Box::new(MutableFixedSizeBinaryArray::with_capacity(
                *size as usize,
                capacity,
            )) as Box<dyn MutableArray>,
            other => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Deserializing type {:?} is still not implemented",
                    other
                )))
            }
        },
    })
}

#[inline]
fn deserialize_item<'a>(
    array: &mut dyn MutableArray,
    is_nullable: bool,
    mut block: &'a [u8],
) -> Result<&'a [u8]> {
    if is_nullable {
        // variant 0 is always the null in a union array
        if util::zigzag_i64(&mut block)? == 0 {
            array.push_null();
            return Ok(block);
        }
    }

    let data_type = array.data_type();
    match data_type {
        DataType::List(inner) => {
            let is_nullable = inner.is_nullable();
            let array = array
                .as_mut_any()
                .downcast_mut::<DynMutableListArray<i32>>()
                .unwrap();
            loop {
                let len = util::zigzag_i64(&mut block)? as usize;

                if len == 0 {
                    break;
                }

                let values = array.mut_values();
                for _ in 0..len {
                    block = deserialize_item(values, is_nullable, block)?;
                }
                array.try_push_valid()?;
            }
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            // https://avro.apache.org/docs/current/spec.html#Duration
            // 12 bytes, months, days, millis in LE
            let data = &block[..12];
            block = &block[12..];

            let value = months_days_ns::new(
                i32::from_le_bytes([data[0], data[1], data[2], data[3]]),
                i32::from_le_bytes([data[4], data[5], data[6], data[7]]),
                i32::from_le_bytes([data[8], data[9], data[10], data[11]]) as i64 * 1_000_000,
            );

            let array = array
                .as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<months_days_ns>>()
                .unwrap();
            array.push(Some(value))
        }
        _ => match data_type.to_physical_type() {
            PhysicalType::Boolean => {
                let is_valid = block[0] == 1;
                block = &block[1..];
                let array = array
                    .as_mut_any()
                    .downcast_mut::<MutableBooleanArray>()
                    .unwrap();
                array.push(Some(is_valid))
            }
            PhysicalType::Primitive(primitive) => match primitive {
                PrimitiveType::Int32 => {
                    let value = util::zigzag_i64(&mut block)? as i32;
                    let array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<i32>>()
                        .unwrap();
                    array.push(Some(value))
                }
                PrimitiveType::Int64 => {
                    let value = util::zigzag_i64(&mut block)? as i64;
                    let array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<i64>>()
                        .unwrap();
                    array.push(Some(value))
                }
                PrimitiveType::Float32 => {
                    let value =
                        f32::from_le_bytes(block[..std::mem::size_of::<f32>()].try_into().unwrap());
                    block = &block[std::mem::size_of::<f32>()..];
                    let array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<f32>>()
                        .unwrap();
                    array.push(Some(value))
                }
                PrimitiveType::Float64 => {
                    let value =
                        f64::from_le_bytes(block[..std::mem::size_of::<f64>()].try_into().unwrap());
                    block = &block[std::mem::size_of::<f64>()..];
                    let array = array
                        .as_mut_any()
                        .downcast_mut::<MutablePrimitiveArray<f64>>()
                        .unwrap();
                    array.push(Some(value))
                }
                _ => unreachable!(),
            },
            PhysicalType::Utf8 => {
                let len: usize = util::zigzag_i64(&mut block)?.try_into().map_err(|_| {
                    ArrowError::ExternalFormat(
                        "Avro format contains a non-usize number of bytes".to_string(),
                    )
                })?;
                let data = simdutf8::basic::from_utf8(&block[..len])?;
                block = &block[len..];

                let array = array
                    .as_mut_any()
                    .downcast_mut::<MutableUtf8Array<i32>>()
                    .unwrap();
                array.push(Some(data))
            }
            PhysicalType::Binary => {
                let len: usize = util::zigzag_i64(&mut block)?.try_into().map_err(|_| {
                    ArrowError::ExternalFormat(
                        "Avro format contains a non-usize number of bytes".to_string(),
                    )
                })?;
                let data = &block[..len];
                block = &block[len..];

                let array = array
                    .as_mut_any()
                    .downcast_mut::<MutableBinaryArray<i32>>()
                    .unwrap();
                array.push(Some(data));
            }
            PhysicalType::FixedSizeBinary => {
                let array = array
                    .as_mut_any()
                    .downcast_mut::<MutableFixedSizeBinaryArray>()
                    .unwrap();
                let len = array.size();
                let data = &block[..len];
                block = &block[len..];
                array.push(Some(data));
            }
            PhysicalType::Dictionary(_) => {
                let index = util::zigzag_i64(&mut block)? as i32;
                let array = array
                    .as_mut_any()
                    .downcast_mut::<FixedItemsUtf8Dictionary>()
                    .unwrap();
                array.push_valid(index);
            }
            _ => todo!(),
        },
    };
    Ok(block)
}

pub fn deserialize(
    mut block: &[u8],
    rows: usize,
    schema: Arc<Schema>,
    avro_schemas: &[AvroSchema],
) -> Result<RecordBatch> {
    // create mutables, one per field
    let mut arrays: Vec<Box<dyn MutableArray>> = schema
        .fields()
        .iter()
        .zip(avro_schemas.iter())
        .map(|(field, avro_schema)| {
            let data_type = field.data_type().to_logical_type();
            make_mutable(data_type, Some(avro_schema), rows)
        })
        .collect::<Result<_>>()?;

    // this is _the_ expensive transpose (rows -> columns)
    for _ in 0..rows {
        for (array, field) in arrays.iter_mut().zip(schema.fields().iter()) {
            block = deserialize_item(array.as_mut(), field.is_nullable(), block)?
        }
    }
    let columns = arrays.iter_mut().map(|array| array.as_arc()).collect();

    RecordBatch::try_new(schema, columns)
}
