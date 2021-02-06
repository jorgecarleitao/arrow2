use std::unimplemented;

use crate::{
    buffers::Bitmap,
    datatypes::{DataType, IntervalUnit},
};

use super::{primitive::PrimitiveArray, Array};

mod primitive;
mod utils;

/// Compares the values of two [ArrayData] starting at `lhs_start` and `rhs_start` respectively
/// for `len` slots. The null buffers `lhs_nulls` and `rhs_nulls` inherit parent nullability.
///
/// If an array is a child of a struct or list, the array's nulls have to be merged with the parent.
/// This then affects the null count of the array, thus the merged nulls are passed separately
/// as `lhs_nulls` and `rhs_nulls` variables to functions.
/// The nulls are merged with a bitwise AND, and null counts are recomputed where necessary.
#[inline]
fn equal_values(
    lhs: &dyn Array,
    rhs: &dyn Array,
    lhs_nulls: &Option<Bitmap>,
    rhs_nulls: &Option<Bitmap>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    match lhs.data_type() {
        DataType::UInt8 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
            primitive::equal(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        }
        DataType::UInt16 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap();
            primitive::equal(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        }
        DataType::UInt32 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();
            primitive::equal(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        }
        DataType::UInt64 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
            primitive::equal(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        }
        DataType::Int8 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<i8>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<i8>>().unwrap();
            primitive::equal(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        }
        DataType::Int16 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<i16>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<i16>>().unwrap();
            primitive::equal(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
            primitive::equal(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Interval(IntervalUnit::DayTime)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
            primitive::equal(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        }
        DataType::Float32 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
            primitive::equal(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        }
        DataType::Float64 => {
            let lhs = lhs.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
            primitive::equal(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        }
        DataType::Float16 => unreachable!(),
        _ => unimplemented!(),
    }
}

/// Logically compares two [ArrayData].
/// Two arrays are logically equal if and only if:
/// * their data types are equal
/// * their lengths are equal
/// * their null counts are equal
/// * their null bitmaps are equal
/// * each of their items are equal
/// two items are equal when their in-memory representation is physically equal (i.e. same bit content).
/// The physical comparison depend on the data type.
/// # Panics
/// This function may panic whenever any of the [ArrayData] does not follow the Arrow specification.
/// (e.g. wrong number of buffers, buffer `len` does not correspond to the declared `len`)
pub fn equal(lhs: &dyn Array, rhs: &dyn Array) -> bool {
    let lhs_nulls = lhs.nulls();
    let rhs_nulls = rhs.nulls();
    utils::base_equal(lhs, rhs)
        && lhs.null_count() == rhs.null_count()
        && utils::equal_nulls(lhs_nulls, rhs_nulls, 0, 0, lhs.len())
        && equal_values(lhs, rhs, lhs_nulls, rhs_nulls, 0, 0, lhs.len())
}
