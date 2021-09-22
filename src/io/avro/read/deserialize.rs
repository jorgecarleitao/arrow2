use std::convert::TryInto;
use std::sync::Arc;

use crate::array::*;
use crate::datatypes::*;
use crate::error::ArrowError;
use crate::error::Result;
use crate::record_batch::RecordBatch;

use super::util;

pub fn deserialize(mut block: &[u8], rows: usize, schema: Arc<Schema>) -> Result<RecordBatch> {
    // create mutables, one per field
    let mut arrays: Vec<Box<dyn MutableArray>> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type().to_physical_type() {
            PhysicalType::Boolean => {
                Ok(Box::new(MutableBooleanArray::with_capacity(rows)) as Box<dyn MutableArray>)
            }
            PhysicalType::Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
                Ok(Box::new(MutablePrimitiveArray::<$T>::with_capacity(rows)) as Box<dyn MutableArray>)
            }),
            PhysicalType::Utf8 => {
                Ok(Box::new(MutableUtf8Array::<i32>::with_capacity(rows)) as Box<dyn MutableArray>)
            }
            PhysicalType::Binary => {
                Ok(Box::new(MutableBinaryArray::<i32>::with_capacity(rows))
                    as Box<dyn MutableArray>)
            }
            other => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Deserializing type {:?} is still not implemented",
                    other
                )))
            }
        })
        .collect::<Result<_>>()?;

    // this is _the_ expensive transpose (rows -> columns)
    for _ in 0..rows {
        for (array, field) in arrays.iter_mut().zip(schema.fields().iter()) {
            if field.is_nullable() {
                // variant 0 is always the null in a union array
                if util::zigzag_i64(&mut block)? == 0 {
                    array.push_null();
                    continue;
                }
            }

            match array.data_type().to_physical_type() {
                PhysicalType::Boolean => {
                    let is_valid = block[0] == 1;
                    block = &block[1..];
                    let array = array
                        .as_mut_any()
                        .downcast_mut::<MutableBooleanArray>()
                        .unwrap();
                    array.push(Some(is_valid))
                }
                PhysicalType::Primitive(primitive) => {
                    use crate::datatypes::PrimitiveType::*;
                    match primitive {
                        Int32 => {
                            let value = util::zigzag_i64(&mut block)? as i32;
                            let array = array
                                .as_mut_any()
                                .downcast_mut::<MutablePrimitiveArray<i32>>()
                                .unwrap();
                            array.push(Some(value))
                        }
                        Int64 => {
                            let value = util::zigzag_i64(&mut block)? as i64;
                            let array = array
                                .as_mut_any()
                                .downcast_mut::<MutablePrimitiveArray<i64>>()
                                .unwrap();
                            array.push(Some(value))
                        }
                        Float32 => {
                            let value = f32::from_le_bytes(block[..4].try_into().unwrap());
                            block = &block[4..];
                            let array = array
                                .as_mut_any()
                                .downcast_mut::<MutablePrimitiveArray<f32>>()
                                .unwrap();
                            array.push(Some(value))
                        }
                        Float64 => {
                            let value = f64::from_le_bytes(block[..8].try_into().unwrap());
                            block = &block[8..];
                            let array = array
                                .as_mut_any()
                                .downcast_mut::<MutablePrimitiveArray<f64>>()
                                .unwrap();
                            array.push(Some(value))
                        }
                        _ => unreachable!(),
                    }
                }
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
                    array.push(Some(data))
                }
                _ => todo!(),
            };
        }
    }
    let columns = arrays.iter_mut().map(|array| array.as_arc()).collect();

    RecordBatch::try_new(schema, columns)
}
