//! Contains functions and function factories to order values within arrays.

use std::cmp::Ordering;

use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::{
    array::*,
    types::{days_ms, NativeType},
};

/// Compare the values at two arbitrary indices in two arrays.
pub type DynComparator<'a> = Box<dyn Fn(usize, usize) -> Ordering + 'a>;

/// implements comparison using IEEE 754 total ordering for f32
// Original implementation from https://doc.rust-lang.org/std/primitive.f32.html#method.total_cmp
// TODO to change to use std when it becomes stable
#[inline]
pub fn total_cmp_f32(l: &f32, r: &f32) -> std::cmp::Ordering {
    let mut left = l.to_bits() as i32;
    let mut right = r.to_bits() as i32;

    left ^= (((left >> 31) as u32) >> 1) as i32;
    right ^= (((right >> 31) as u32) >> 1) as i32;

    left.cmp(&right)
}

/// implements comparison using IEEE 754 total ordering for f64
// Original implementation from https://doc.rust-lang.org/std/primitive.f64.html#method.total_cmp
// TODO to change to use std when it becomes stable
#[inline]
pub fn total_cmp_f64(l: &f64, r: &f64) -> std::cmp::Ordering {
    let mut left = l.to_bits() as i64;
    let mut right = r.to_bits() as i64;

    left ^= (((left >> 63) as u64) >> 1) as i64;
    right ^= (((right >> 63) as u64) >> 1) as i64;

    left.cmp(&right)
}

/// Total order of all native types whose Rust implementation
/// that support total order.
#[inline]
pub fn total_cmp<T>(l: &T, r: &T) -> std::cmp::Ordering
where
    T: NativeType + Ord,
{
    l.cmp(r)
}

fn compare_primitives<'a, T: NativeType + Ord>(
    left: &'a dyn Array,
    right: &'a dyn Array,
) -> DynComparator<'a> {
    let left = left.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let right = right.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let left = left.values();
    let right = right.values();
    Box::new(move |i, j| total_cmp(&left[i], &right[j]))
}

fn compare_boolean<'a>(left: &'a dyn Array, right: &'a dyn Array) -> DynComparator<'a> {
    let left = left.as_any().downcast_ref::<BooleanArray>().unwrap();
    let right = right.as_any().downcast_ref::<BooleanArray>().unwrap();
    Box::new(move |i, j| left.value(i).cmp(&right.value(j)))
}

fn compare_f32<'a>(left: &'a dyn Array, right: &'a dyn Array) -> DynComparator<'a> {
    let left = left.as_any().downcast_ref::<PrimitiveArray<f32>>().unwrap();
    let right = right
        .as_any()
        .downcast_ref::<PrimitiveArray<f32>>()
        .unwrap();
    let left = left.values();
    let right = right.values();
    Box::new(move |i, j| total_cmp_f32(&left[i], &right[j]))
}

fn compare_f64<'a>(left: &'a dyn Array, right: &'a dyn Array) -> DynComparator<'a> {
    let left = left.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
    let right = right
        .as_any()
        .downcast_ref::<PrimitiveArray<f64>>()
        .unwrap();
    let left = left.values();
    let right = right.values();
    Box::new(move |i, j| total_cmp_f64(&left[i], &right[j]))
}

fn compare_string<'a, O: Offset>(left: &'a dyn Array, right: &'a dyn Array) -> DynComparator<'a> {
    let left = left.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
    let right = right.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
    Box::new(move |i, j| left.value(i).cmp(&right.value(j)))
}

fn compare_dict<'a, K>(
    left: &'a DictionaryArray<K>,
    right: &'a DictionaryArray<K>,
) -> Result<DynComparator<'a>>
where
    K: DictionaryKey,
{
    let left_keys = left.keys().values();
    let right_keys = right.keys().values();

    let comparator = build_compare(left.values().as_ref(), right.values().as_ref())?;

    Ok(Box::new(move |i: usize, j: usize| {
        let key_left = left_keys[i].to_usize().unwrap();
        let key_right = right_keys[j].to_usize().unwrap();
        (comparator)(key_left, key_right)
    }))
}

macro_rules! dyn_dict {
    ($key:ty, $lhs:expr, $rhs:expr) => {{
        let lhs = $lhs.as_any().downcast_ref().unwrap();
        let rhs = $rhs.as_any().downcast_ref().unwrap();
        compare_dict::<$key>(lhs, rhs)?
    }};
}

/// returns a comparison function that compares values at two different slots
/// between two [`Array`].
/// # Example
/// ```
/// use arrow2::array::{ord::build_compare, PrimitiveArray};
/// use arrow2::datatypes::DataType;
///
/// # fn main() -> arrow2::error::Result<()> {
/// let array1 = PrimitiveArray::from_slice(&[1, 2]).to(DataType::Int32);
/// let array2 = PrimitiveArray::from_slice(&[3, 4]).to(DataType::Int32);
///
/// let cmp = build_compare(&array1, &array2)?;
///
/// // 1 (index 0 of array1) is smaller than 4 (index 1 of array2)
/// assert_eq!(std::cmp::Ordering::Less, (cmp)(0, 1));
/// # Ok(())
/// # }
/// ```
/// # Error
/// The arrays' [`DataType`] must be equal and the types must have a natural order.
// This is a factory of comparisons.
// The lifetime 'a enforces that we cannot use the closure beyond any of the array's lifetime.
pub fn build_compare<'a>(left: &'a dyn Array, right: &'a dyn Array) -> Result<DynComparator<'a>> {
    use DataType::*;
    use IntervalUnit::*;
    use TimeUnit::*;
    Ok(match (left.data_type(), right.data_type()) {
        (a, b) if a != b => {
            return Err(ArrowError::InvalidArgumentError(
                "Can't compare arrays of different types".to_string(),
            ));
        }
        (Boolean, Boolean) => compare_boolean(left, right),
        (UInt8, UInt8) => compare_primitives::<u8>(left, right),
        (UInt16, UInt16) => compare_primitives::<u16>(left, right),
        (UInt32, UInt32) => compare_primitives::<u32>(left, right),
        (UInt64, UInt64) => compare_primitives::<u64>(left, right),
        (Int8, Int8) => compare_primitives::<i8>(left, right),
        (Int16, Int16) => compare_primitives::<i16>(left, right),
        (Int32, Int32)
        | (Date32, Date32)
        | (Time32(Second), Time32(Second))
        | (Time32(Millisecond), Time32(Millisecond))
        | (Interval(YearMonth), Interval(YearMonth)) => compare_primitives::<i32>(left, right),
        (Int64, Int64)
        | (Date64, Date64)
        | (Time64(Microsecond), Time64(Microsecond))
        | (Time64(Nanosecond), Time64(Nanosecond))
        | (Timestamp(Second, None), Timestamp(Second, None))
        | (Timestamp(Millisecond, None), Timestamp(Millisecond, None))
        | (Timestamp(Microsecond, None), Timestamp(Microsecond, None))
        | (Timestamp(Nanosecond, None), Timestamp(Nanosecond, None))
        | (Duration(Second), Duration(Second))
        | (Duration(Millisecond), Duration(Millisecond))
        | (Duration(Microsecond), Duration(Microsecond))
        | (Duration(Nanosecond), Duration(Nanosecond)) => compare_primitives::<i64>(left, right),
        (Float32, Float32) => compare_f32(left, right),
        (Float64, Float64) => compare_f64(left, right),
        (Interval(DayTime), Interval(DayTime)) => compare_primitives::<days_ms>(left, right),
        (Utf8, Utf8) => compare_string::<i32>(left, right),
        (LargeUtf8, LargeUtf8) => compare_string::<i64>(left, right),
        (Dictionary(key_type_lhs, _), Dictionary(key_type_rhs, _)) => {
            match (key_type_lhs.as_ref(), key_type_rhs.as_ref()) {
                (UInt8, UInt8) => dyn_dict!(u8, left, right),
                (UInt16, UInt16) => dyn_dict!(u16, left, right),
                (UInt32, UInt32) => dyn_dict!(u32, left, right),
                (UInt64, UInt64) => dyn_dict!(u64, left, right),
                (Int8, Int8) => dyn_dict!(i8, left, right),
                (Int16, Int16) => dyn_dict!(i16, left, right),
                (Int32, Int32) => dyn_dict!(i32, left, right),
                (Int64, Int64) => dyn_dict!(i64, left, right),
                (lhs, _) => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Dictionaries do not support keys of type {:?}",
                        lhs
                    )))
                }
            }
        }
        (lhs, _) => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "The data type type {:?} has no natural order",
                lhs
            )))
        }
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::error::Result;
    use std::cmp::Ordering;

    #[test]
    fn test_i32() -> Result<()> {
        let array = Int32Array::from_slice(&[1, 2]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_i32_i32() -> Result<()> {
        let array1 = Int32Array::from_slice(&[1]);
        let array2 = Int32Array::from_slice(&[2]);

        let cmp = build_compare(&array1, &array2)?;

        assert_eq!(Ordering::Less, (cmp)(0, 0));
        Ok(())
    }

    #[test]
    fn test_f32() -> Result<()> {
        let array = &Float32Array::from_slice(&[1.0, 2.0]);

        let cmp = build_compare(array, array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_f64() -> Result<()> {
        let array = Float64Array::from_slice(&[1.0, 2.0]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_f64_nan() -> Result<()> {
        let array = Float64Array::from_slice(&[1.0, f64::NAN]);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_f64_zeros() -> Result<()> {
        let array = Float64Array::from_slice(&[-0.0, 0.0]);

        let cmp = build_compare(&array, &array)?;

        // official IEEE 754 (2008 revision)
        assert_eq!(Ordering::Less, (cmp)(0, 1));
        assert_eq!(Ordering::Greater, (cmp)(1, 0));
        Ok(())
    }

    #[test]
    fn test_dict() -> Result<()> {
        let data = vec!["a", "b", "c", "a", "a", "c", "c"];

        let data = data.into_iter().map(Some);
        let mut array = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
        array.try_extend(data)?;
        let array: DictionaryArray<i32> = array.into();

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        assert_eq!(Ordering::Equal, (cmp)(3, 4));
        assert_eq!(Ordering::Greater, (cmp)(2, 3));
        Ok(())
    }

    #[test]
    fn test_dict_1() -> Result<()> {
        let data = vec![1, 2, 3, 1, 1, 3, 3];

        let data = data.into_iter().map(Some);

        let mut array = MutableDictionaryArray::<i32, MutablePrimitiveArray<i32>>::new();
        array.try_extend(data)?;
        let array = array.into_arc();

        let cmp = build_compare(array.as_ref(), array.as_ref())?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        assert_eq!(Ordering::Equal, (cmp)(3, 4));
        assert_eq!(Ordering::Greater, (cmp)(2, 3));
        Ok(())
    }
}
