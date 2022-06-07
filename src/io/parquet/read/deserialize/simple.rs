use std::sync::Arc;

use parquet2::{
    schema::types::{
        PhysicalType, PrimitiveLogicalType, PrimitiveType, TimeUnit as ParquetTimeUnit,
    },
    types::int96_to_i64_ns,
};

use crate::{
    array::{Array, BinaryArray, DictionaryKey, MutablePrimitiveArray, PrimitiveArray, Utf8Array},
    datatypes::{DataType, IntervalUnit, TimeUnit},
    error::{Error, Result},
    types::NativeType,
};

use super::super::{ArrayIter, DataPages};
use super::binary;
use super::boolean;
use super::fixed_size_binary;
use super::null;
use super::primitive;

/// Converts an iterator of arrays to a trait object returning trait objects
#[inline]
fn dyn_iter<'a, A, I>(iter: I) -> ArrayIter<'a>
where
    A: Array + 'static,
    I: Iterator<Item = Result<A>> + Send + Sync + 'a,
{
    Box::new(iter.map(|x| x.map(|x| Arc::new(x) as Arc<dyn Array>)))
}

/// Converts an iterator of [MutablePrimitiveArray] into an iterator of [PrimitiveArray]
#[inline]
fn iden<T, I>(iter: I) -> impl Iterator<Item = Result<PrimitiveArray<T>>>
where
    T: NativeType,
    I: Iterator<Item = Result<MutablePrimitiveArray<T>>>,
{
    iter.map(|x| x.map(|x| x.into()))
}

#[inline]
fn op<T, I, F>(iter: I, op: F) -> impl Iterator<Item = Result<PrimitiveArray<T>>>
where
    T: NativeType,
    I: Iterator<Item = Result<MutablePrimitiveArray<T>>>,
    F: Fn(T) -> T + Copy,
{
    iter.map(move |x| {
        x.map(move |mut x| {
            x.values_mut_slice().iter_mut().for_each(|x| *x = op(*x));
            x.into()
        })
    })
}

/// An iterator adapter that maps an iterator of DataPages into an iterator of Arrays
/// of [`DataType`] `data_type` and `chunk_size`.
pub fn page_iter_to_arrays<'a, I: 'a + DataPages>(
    pages: I,
    type_: &PrimitiveType,
    data_type: DataType,
    chunk_size: Option<usize>,
) -> Result<ArrayIter<'a>> {
    use DataType::*;

    let physical_type = &type_.physical_type;
    let logical_type = &type_.logical_type;

    Ok(match data_type.to_logical_type() {
        Null => null::iter_to_arrays(pages, data_type, chunk_size),
        Boolean => dyn_iter(boolean::Iter::new(pages, data_type, chunk_size)),
        UInt8 => dyn_iter(iden(primitive::Iter::new(
            pages,
            data_type,
            chunk_size,
            |x: i32| x as u8,
        ))),
        UInt16 => dyn_iter(iden(primitive::Iter::new(
            pages,
            data_type,
            chunk_size,
            |x: i32| x as u16,
        ))),
        UInt32 => dyn_iter(iden(primitive::Iter::new(
            pages,
            data_type,
            chunk_size,
            |x: i32| x as u32,
        ))),
        Int8 => dyn_iter(iden(primitive::Iter::new(
            pages,
            data_type,
            chunk_size,
            |x: i32| x as i8,
        ))),
        Int16 => dyn_iter(iden(primitive::Iter::new(
            pages,
            data_type,
            chunk_size,
            |x: i32| x as i16,
        ))),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => dyn_iter(iden(
            primitive::Iter::new(pages, data_type, chunk_size, |x: i32| x as i32),
        )),

        Timestamp(time_unit, _) => {
            let time_unit = *time_unit;
            return timestamp(
                pages,
                physical_type,
                logical_type,
                data_type,
                chunk_size,
                time_unit,
            );
        }

        FixedSizeBinary(_) => dyn_iter(fixed_size_binary::Iter::new(pages, data_type, chunk_size)),

        Decimal(_, _) => match physical_type {
            PhysicalType::Int32 => dyn_iter(iden(primitive::Iter::new(
                pages,
                data_type,
                chunk_size,
                |x: i32| x as i128,
            ))),
            PhysicalType::Int64 => dyn_iter(iden(primitive::Iter::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x as i128,
            ))),
            &PhysicalType::FixedLenByteArray(n) if n > 16 => {
                return Err(Error::NotYetImplemented(format!(
                    "Can't decode Decimal128 type from Fixed Size Byte Array of len {:?}",
                    n
                )))
            }
            &PhysicalType::FixedLenByteArray(n) => {
                let n = n as usize;

                let pages =
                    fixed_size_binary::Iter::new(pages, DataType::FixedSizeBinary(n), chunk_size);

                let pages = pages.map(move |maybe_array| {
                    let array = maybe_array?;
                    let values = array
                        .values()
                        .chunks_exact(n)
                        .map(|value: &[u8]| {
                            // Copy the fixed-size byte value to the start of a 16 byte stack
                            // allocated buffer, then use an arithmetic right shift to fill in
                            // MSBs, which accounts for leading 1's in negative (two's complement)
                            // values.
                            let mut bytes = [0u8; 16];
                            bytes[..n].copy_from_slice(value);
                            i128::from_be_bytes(bytes) >> (8 * (16 - n))
                        })
                        .collect::<Vec<_>>();
                    let validity = array.validity().cloned();

                    PrimitiveArray::<i128>::try_new(data_type.clone(), values.into(), validity)
                });

                let arrays = pages.map(|x| x.map(|x| x.arced()));

                Box::new(arrays) as _
            }
            _ => unreachable!(),
        },

        // INT64
        Int64 | Date64 | Time64(_) | Duration(_) => dyn_iter(iden(primitive::Iter::new(
            pages,
            data_type,
            chunk_size,
            |x: i64| x as i64,
        ))),
        UInt64 => dyn_iter(iden(primitive::Iter::new(
            pages,
            data_type,
            chunk_size,
            |x: i64| x as u64,
        ))),

        Float32 => dyn_iter(iden(primitive::Iter::new(
            pages,
            data_type,
            chunk_size,
            |x: f32| x,
        ))),
        Float64 => dyn_iter(iden(primitive::Iter::new(
            pages,
            data_type,
            chunk_size,
            |x: f64| x,
        ))),

        Binary => dyn_iter(binary::Iter::<i32, BinaryArray<i32>, _>::new(
            pages, data_type, chunk_size,
        )),
        LargeBinary => dyn_iter(binary::Iter::<i64, BinaryArray<i64>, _>::new(
            pages, data_type, chunk_size,
        )),
        Utf8 => dyn_iter(binary::Iter::<i32, Utf8Array<i32>, _>::new(
            pages, data_type, chunk_size,
        )),
        LargeUtf8 => dyn_iter(binary::Iter::<i64, Utf8Array<i64>, _>::new(
            pages, data_type, chunk_size,
        )),

        Dictionary(key_type, _, _) => {
            return match_integer_type!(key_type, |$K| {
                dict_read::<$K, _>(pages, physical_type, logical_type, data_type, chunk_size)
            })
        }

        other => {
            return Err(Error::NotYetImplemented(format!(
                "Reading {:?} from parquet still not implemented",
                other
            )))
        }
    })
}

fn timestamp<'a, I: 'a + DataPages>(
    pages: I,
    physical_type: &PhysicalType,
    logical_type: &Option<PrimitiveLogicalType>,
    data_type: DataType,
    chunk_size: Option<usize>,
    time_unit: TimeUnit,
) -> Result<ArrayIter<'a>> {
    if physical_type == &PhysicalType::Int96 {
        if time_unit == TimeUnit::Nanosecond {
            return Ok(dyn_iter(iden(primitive::Iter::new(
                pages,
                data_type,
                chunk_size,
                int96_to_i64_ns,
            ))));
        } else {
            return Err(Error::nyi("Can't decode int96 to timestamp other than ns"));
        }
    };
    if physical_type != &PhysicalType::Int64 {
        return Err(Error::nyi(
            "Can't decode a timestamp from a non-int64 parquet type",
        ));
    }

    let iter = primitive::Iter::new(pages, data_type, chunk_size, |x: i64| x);

    let unit = if let Some(PrimitiveLogicalType::Timestamp { unit, .. }) = logical_type {
        unit
    } else {
        return Ok(dyn_iter(iden(iter)));
    };

    Ok(match (unit, time_unit) {
        (ParquetTimeUnit::Milliseconds, TimeUnit::Second) => dyn_iter(op(iter, |x| x / 1_000)),
        (ParquetTimeUnit::Microseconds, TimeUnit::Second) => dyn_iter(op(iter, |x| x / 1_000_000)),
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Second) => {
            dyn_iter(op(iter, |x| x / 1_000_000_000))
        }

        (ParquetTimeUnit::Milliseconds, TimeUnit::Millisecond) => dyn_iter(iden(iter)),
        (ParquetTimeUnit::Microseconds, TimeUnit::Millisecond) => dyn_iter(op(iter, |x| x / 1_000)),
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Millisecond) => {
            dyn_iter(op(iter, |x| x / 1_000_000))
        }

        (ParquetTimeUnit::Milliseconds, TimeUnit::Microsecond) => dyn_iter(op(iter, |x| x * 1_000)),
        (ParquetTimeUnit::Microseconds, TimeUnit::Microsecond) => dyn_iter(iden(iter)),
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Microsecond) => dyn_iter(op(iter, |x| x / 1_000)),

        (ParquetTimeUnit::Milliseconds, TimeUnit::Nanosecond) => {
            dyn_iter(op(iter, |x| x * 1_000_000))
        }
        (ParquetTimeUnit::Microseconds, TimeUnit::Nanosecond) => dyn_iter(op(iter, |x| x * 1_000)),
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Nanosecond) => dyn_iter(iden(iter)),
    })
}

fn timestamp_dict<'a, K: DictionaryKey, I: 'a + DataPages>(
    pages: I,
    physical_type: &PhysicalType,
    logical_type: &Option<PrimitiveLogicalType>,
    data_type: DataType,
    chunk_size: Option<usize>,
    time_unit: TimeUnit,
) -> Result<ArrayIter<'a>> {
    if physical_type == &PhysicalType::Int96 {
        if time_unit == TimeUnit::Nanosecond {
            return Ok(dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                chunk_size,
                int96_to_i64_ns,
            )));
        } else {
            return Err(Error::nyi("Can't decode int96 to timestamp other than ns"));
        }
    };

    let unit = if let Some(PrimitiveLogicalType::Timestamp { unit, .. }) = logical_type {
        unit
    } else {
        return Ok(dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
            pages,
            data_type,
            chunk_size,
            |x: i64| x,
        )));
    };

    Ok(match (unit, time_unit) {
        (ParquetTimeUnit::Milliseconds, TimeUnit::Second) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x / 1_000,
            ))
        }
        (ParquetTimeUnit::Microseconds, TimeUnit::Second) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x / 1_000_000,
            ))
        }
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Second) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x / 1_000_000_000,
            ))
        }

        (ParquetTimeUnit::Milliseconds, TimeUnit::Millisecond) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x,
            ))
        }
        (ParquetTimeUnit::Microseconds, TimeUnit::Millisecond) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x / 1_000,
            ))
        }
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Millisecond) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x / 1_000_000,
            ))
        }

        (ParquetTimeUnit::Milliseconds, TimeUnit::Microsecond) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x * 1_000,
            ))
        }
        (ParquetTimeUnit::Microseconds, TimeUnit::Microsecond) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x,
            ))
        }
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Microsecond) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x / 1_000,
            ))
        }

        (ParquetTimeUnit::Milliseconds, TimeUnit::Nanosecond) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x * 1_000_000,
            ))
        }
        (ParquetTimeUnit::Microseconds, TimeUnit::Nanosecond) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x * 1_000,
            ))
        }
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Nanosecond) => {
            dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
                pages,
                data_type,
                chunk_size,
                |x: i64| x,
            ))
        }
    })
}

fn dict_read<'a, K: DictionaryKey, I: 'a + DataPages>(
    iter: I,
    physical_type: &PhysicalType,
    logical_type: &Option<PrimitiveLogicalType>,
    data_type: DataType,
    chunk_size: Option<usize>,
) -> Result<ArrayIter<'a>> {
    use DataType::*;
    let values_data_type = if let Dictionary(_, v, _) = &data_type {
        v.as_ref()
    } else {
        panic!()
    };

    Ok(match values_data_type.to_logical_type() {
        UInt8 => dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
            iter,
            data_type,
            chunk_size,
            |x: i32| x as u8,
        )),
        UInt16 => dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
            iter,
            data_type,
            chunk_size,
            |x: i32| x as u16,
        )),
        UInt32 => dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
            iter,
            data_type,
            chunk_size,
            |x: i32| x as u32,
        )),
        Int8 => dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
            iter,
            data_type,
            chunk_size,
            |x: i32| x as i8,
        )),
        Int16 => dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
            iter,
            data_type,
            chunk_size,
            |x: i32| x as i16,
        )),
        Int32 | Date32 | Time32(_) | Interval(IntervalUnit::YearMonth) => dyn_iter(
            primitive::DictIter::<K, _, _, _, _>::new(iter, data_type, chunk_size, |x: i32| {
                x as i32
            }),
        ),

        Timestamp(time_unit, _) => {
            let time_unit = *time_unit;
            return timestamp_dict::<K, _>(
                iter,
                physical_type,
                logical_type,
                data_type,
                chunk_size,
                time_unit,
            );
        }

        Int64 | Date64 | Time64(_) | Duration(_) => dyn_iter(
            primitive::DictIter::<K, _, _, _, _>::new(iter, data_type, chunk_size, |x: i64| x),
        ),
        Float32 => dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
            iter,
            data_type,
            chunk_size,
            |x: f32| x,
        )),
        Float64 => dyn_iter(primitive::DictIter::<K, _, _, _, _>::new(
            iter,
            data_type,
            chunk_size,
            |x: f64| x,
        )),

        Utf8 | Binary => dyn_iter(binary::DictIter::<K, i32, _>::new(
            iter, data_type, chunk_size,
        )),
        LargeUtf8 | LargeBinary => dyn_iter(binary::DictIter::<K, i64, _>::new(
            iter, data_type, chunk_size,
        )),
        FixedSizeBinary(_) => dyn_iter(fixed_size_binary::DictIter::<K, _>::new(
            iter, data_type, chunk_size,
        )),
        other => {
            return Err(Error::nyi(format!(
                "Reading dictionaries of type {:?}",
                other
            )))
        }
    })
}
