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

// implements comparison using IEEE 754 total ordering for f32
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

// implements comparison using IEEE 754 total ordering for f64
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
/// supports total order.
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

fn compare_dict_string<'a, T>(left: &'a dyn Array, right: &'a dyn Array) -> DynComparator<'a>
where
    T: DictionaryKey,
{
    let left = left.as_any().downcast_ref::<DictionaryArray<T>>().unwrap();
    let right = right.as_any().downcast_ref::<DictionaryArray<T>>().unwrap();
    let left_keys = left.keys().values();
    let right_keys = right.keys().values();

    let left_values = left
        .values()
        .as_any()
        .downcast_ref::<Utf8Array<i32>>()
        .unwrap();
    let right_values = right
        .values()
        .as_any()
        .downcast_ref::<Utf8Array<i32>>()
        .unwrap();

    Box::new(move |i: usize, j: usize| {
        let key_left = left_keys[i].to_usize().unwrap();
        let key_right = right_keys[j].to_usize().unwrap();
        let left = left_values.value(key_left);
        let right = right_values.value(key_right);
        left.cmp(&right)
    })
}

/// returns a comparison function that compares two values at two different positions
/// between the two arrays.
/// The arrays' types must be equal.
/// # Example
/// ```
/// use arrow2::array::{ord::build_compare, Primitive};
/// use arrow2::datatypes::DataType;
///
/// # fn main() -> arrow2::error::Result<()> {
/// let array1 = Primitive::from_slice(&[1, 2]).to(DataType::Int32);
/// let array2 = Primitive::from_slice(&[3, 4]).to(DataType::Int32);
///
/// let cmp = build_compare(&array1, &array2)?;
///
/// // 1 (index 0 of array1) is smaller than 4 (index 1 of array2)
/// assert_eq!(std::cmp::Ordering::Less, (cmp)(0, 1));
/// # Ok(())
/// # }
/// ```
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
        (Dictionary(key_type_lhs, value_type_lhs), Dictionary(key_type_rhs, value_type_rhs)) => {
            if value_type_lhs.as_ref() != &DataType::Utf8
                || value_type_rhs.as_ref() != &DataType::Utf8
            {
                return Err(ArrowError::InvalidArgumentError(
                    "Arrow still does not support comparisons of non-string dictionary arrays"
                        .to_string(),
                ));
            }
            match (key_type_lhs.as_ref(), key_type_rhs.as_ref()) {
                (a, b) if a != b => {
                    return Err(ArrowError::InvalidArgumentError(
                        "Can't compare arrays of different types".to_string(),
                    ));
                }
                (UInt8, UInt8) => compare_dict_string::<u8>(left, right),
                (UInt16, UInt16) => compare_dict_string::<u16>(left, right),
                (UInt32, UInt32) => compare_dict_string::<u32>(left, right),
                (UInt64, UInt64) => compare_dict_string::<u64>(left, right),
                (Int8, Int8) => compare_dict_string::<i8>(left, right),
                (Int16, Int16) => compare_dict_string::<i16>(left, right),
                (Int32, Int32) => compare_dict_string::<i32>(left, right),
                (Int64, Int64) => compare_dict_string::<i64>(left, right),
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
        let array = Primitive::from_slice(vec![1, 2]).to(DataType::Int32);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_i32_i32() -> Result<()> {
        let array1 = Primitive::from_slice(&vec![1]).to(DataType::Int32);
        let array2 = Primitive::from_slice(&vec![2]).to(DataType::Int32);

        let cmp = build_compare(&array1, &array2)?;

        assert_eq!(Ordering::Less, (cmp)(0, 0));
        Ok(())
    }

    #[test]
    fn test_f64() -> Result<()> {
        let array = Primitive::from_slice(&vec![1.0, 2.0]).to(DataType::Float64);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_f64_nan() -> Result<()> {
        let array = Primitive::from_slice(vec![1.0, f64::NAN]).to(DataType::Float64);

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        Ok(())
    }

    #[test]
    fn test_f64_zeros() -> Result<()> {
        let array = Primitive::from_slice(vec![-0.0, 0.0]).to(DataType::Float64);

        let cmp = build_compare(&array, &array)?;

        // official IEEE 754 (2008 revision)
        assert_eq!(Ordering::Less, (cmp)(0, 1));
        assert_eq!(Ordering::Greater, (cmp)(1, 0));
        Ok(())
    }

    #[test]
    fn test_dict() -> Result<()> {
        let data = vec!["a", "b", "c", "a", "a", "c", "c"];

        let data = data.into_iter().map(|x| Result::Ok(Some(x)));
        let array = DictionaryPrimitive::<i32, Utf8Primitive<i32>, &str>::try_from_iter(data)?;
        let array = array.to(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ));

        let cmp = build_compare(&array, &array)?;

        assert_eq!(Ordering::Less, (cmp)(0, 1));
        assert_eq!(Ordering::Equal, (cmp)(3, 4));
        assert_eq!(Ordering::Greater, (cmp)(2, 3));
        Ok(())
    }
}
