//! Defines the multiplication arithmetic kernels for Decimal
//! `PrimitiveArrays`.

use crate::compute::arithmetics::basic::check_same_len;
use crate::{
    array::{Array, PrimitiveArray},
    buffer::Buffer,
    compute::{
        arithmetics::{ArrayCheckedMul, ArrayMul, ArraySaturatingMul},
        arity::{binary, binary_checked},
        utils::combine_validities,
    },
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::{adjusted_precision_scale, get_parameters, max_value, number_digits};

/// Multiply two decimal primitive arrays with the same precision and scale. If
/// the precision and scale is different, then an InvalidArgumentError is
/// returned. This function panics if the multiplied numbers result in a number
/// larger than the possible number for the selected precision.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::mul::mul;
/// use arrow2::array::PrimitiveArray;
/// use arrow2::datatypes::DataType;
///
/// let a = PrimitiveArray::from([Some(1_00i128), Some(1_00i128), None, Some(2_00i128)]).to(DataType::Decimal(5, 2));
/// let b = PrimitiveArray::from([Some(1_00i128), Some(2_00i128), None, Some(2_00i128)]).to(DataType::Decimal(5, 2));
///
/// let result = mul(&a, &b).unwrap();
/// let expected = PrimitiveArray::from([Some(1_00i128), Some(2_00i128), None, Some(4_00i128)]).to(DataType::Decimal(5, 2));
///
/// assert_eq!(result, expected);
/// ```
pub fn mul(lhs: &PrimitiveArray<i128>, rhs: &PrimitiveArray<i128>) -> Result<PrimitiveArray<i128>> {
    let (precision, scale) = get_parameters(lhs.data_type(), rhs.data_type())?;

    let scale = 10i128.pow(scale as u32);
    let max = max_value(precision);

    let op = move |a: i128, b: i128| {
        // The multiplication between i128 can overflow if they are
        // very large numbers. For that reason a checked
        // multiplication is used.
        let res: i128 = a.checked_mul(b).expect("Mayor overflow for multiplication");

        // The multiplication is done using the numbers without scale.
        // The resulting scale of the value has to be corrected by
        // dividing by (10^scale)

        //   111.111 -->      111111
        //   222.222 -->      222222
        // --------          -------
        // 24691.308 <-- 24691308642
        let res = res / scale;

        assert!(
            res.abs() <= max,
            "Overflow in multiplication presented for precision {}",
            precision
        );

        res
    };

    binary(lhs, rhs, lhs.data_type().clone(), op)
}

/// Saturated multiplication of two decimal primitive arrays with the same
/// precision and scale. If the precision and scale is different, then an
/// InvalidArgumentError is returned. If the result from the multiplication is
/// larger than the possible number with the selected precision then the
/// resulted number in the arrow array is the maximum number for the selected
/// precision.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::mul::saturating_mul;
/// use arrow2::array::PrimitiveArray;
/// use arrow2::datatypes::DataType;
///
/// let a = PrimitiveArray::from([Some(999_99i128), Some(1_00i128), None, Some(2_00i128)]).to(DataType::Decimal(5, 2));
/// let b = PrimitiveArray::from([Some(10_00i128), Some(2_00i128), None, Some(2_00i128)]).to(DataType::Decimal(5, 2));
///
/// let result = saturating_mul(&a, &b).unwrap();
/// let expected = PrimitiveArray::from([Some(999_99i128), Some(2_00i128), None, Some(4_00i128)]).to(DataType::Decimal(5, 2));
///
/// assert_eq!(result, expected);
/// ```
pub fn saturating_mul(
    lhs: &PrimitiveArray<i128>,
    rhs: &PrimitiveArray<i128>,
) -> Result<PrimitiveArray<i128>> {
    let (precision, scale) = get_parameters(lhs.data_type(), rhs.data_type())?;

    let scale = 10i128.pow(scale as u32);
    let max = max_value(precision);

    let op = move |a: i128, b: i128| match a.checked_mul(b) {
        Some(res) => {
            let res = res / scale;

            match res {
                res if res.abs() > max => {
                    if res > 0 {
                        max
                    } else {
                        -max
                    }
                }
                _ => res,
            }
        }
        None => max,
    };

    binary(lhs, rhs, lhs.data_type().clone(), op)
}

/// Checked multiplication of two decimal primitive arrays with the same
/// precision and scale. If the precision and scale is different, then an
/// InvalidArgumentError is returned. If the result from the mul is larger than
/// the possible number with the selected precision (overflowing), then the
/// validity for that index is changed to None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::mul::checked_mul;
/// use arrow2::array::PrimitiveArray;
/// use arrow2::datatypes::DataType;
///
/// let a = PrimitiveArray::from([Some(999_99i128), Some(1_00i128), None, Some(2_00i128)]).to(DataType::Decimal(5, 2));
/// let b = PrimitiveArray::from([Some(10_00i128), Some(2_00i128), None, Some(2_00i128)]).to(DataType::Decimal(5, 2));
///
/// let result = checked_mul(&a, &b).unwrap();
/// let expected = PrimitiveArray::from([None, Some(2_00i128), None, Some(4_00i128)]).to(DataType::Decimal(5, 2));
///
/// assert_eq!(result, expected);
/// ```
pub fn checked_mul(
    lhs: &PrimitiveArray<i128>,
    rhs: &PrimitiveArray<i128>,
) -> Result<PrimitiveArray<i128>> {
    let (precision, scale) = get_parameters(lhs.data_type(), rhs.data_type())?;

    let scale = 10i128.pow(scale as u32);
    let max = max_value(precision);

    let op = move |a: i128, b: i128| match a.checked_mul(b) {
        Some(res) => {
            let res = res / scale;

            match res {
                res if res.abs() > max => None,
                _ => Some(res),
            }
        }
        None => None,
    };

    binary_checked(lhs, rhs, lhs.data_type().clone(), op)
}

// Implementation of ArrayMul trait for PrimitiveArrays
impl ArrayMul<PrimitiveArray<i128>> for PrimitiveArray<i128> {
    fn mul(&self, rhs: &PrimitiveArray<i128>) -> Result<Self> {
        mul(self, rhs)
    }
}

// Implementation of ArrayCheckedMul trait for PrimitiveArrays
impl ArrayCheckedMul<PrimitiveArray<i128>> for PrimitiveArray<i128> {
    fn checked_mul(&self, rhs: &PrimitiveArray<i128>) -> Result<Self> {
        checked_mul(self, rhs)
    }
}

// Implementation of ArraySaturatingMul trait for PrimitiveArrays
impl ArraySaturatingMul<PrimitiveArray<i128>> for PrimitiveArray<i128> {
    fn saturating_mul(&self, rhs: &PrimitiveArray<i128>) -> Result<Self> {
        saturating_mul(self, rhs)
    }
}

/// Adaptive multiplication of two decimal primitive arrays with different
/// precision and scale. If the precision and scale is different, then the
/// smallest scale and precision is adjusted to the largest precision and
/// scale. If during the multiplication one of the results is larger than the
/// max possible value, the result precision is changed to the precision of the
/// max value
///
/// ```nocode
///   11111.0    -> 6, 1
///      10.002  -> 5, 3
/// -----------------
///  111132.222  -> 9, 3
/// ```
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::mul::adaptive_mul;
/// use arrow2::array::PrimitiveArray;
/// use arrow2::datatypes::DataType;
///
/// let a = PrimitiveArray::from([Some(11111_0i128), Some(1_0i128)]).to(DataType::Decimal(6, 1));
/// let b = PrimitiveArray::from([Some(10_002i128), Some(2_000i128)]).to(DataType::Decimal(5, 3));
/// let result = adaptive_mul(&a, &b).unwrap();
/// let expected = PrimitiveArray::from([Some(111132_222i128), Some(2_000i128)]).to(DataType::Decimal(9, 3));
///
/// assert_eq!(result, expected);
/// ```
pub fn adaptive_mul(
    lhs: &PrimitiveArray<i128>,
    rhs: &PrimitiveArray<i128>,
) -> Result<PrimitiveArray<i128>> {
    check_same_len(lhs, rhs)?;

    if let (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s)) =
        (lhs.data_type(), rhs.data_type())
    {
        // The resulting precision is mutable because it could change while
        // looping through the iterator
        let (mut res_p, res_s, diff) = adjusted_precision_scale(*lhs_p, *lhs_s, *rhs_p, *rhs_s);

        let mut result = Vec::new();
        for (l, r) in lhs.values().iter().zip(rhs.values().iter()) {
            // Based on the array's scales one of the arguments in the sum has to be shifted
            // to the left to match the final scale
            let res = if lhs_s > rhs_s {
                l.checked_mul(r * 10i128.pow(diff as u32))
                    .expect("Mayor overflow for multiplication")
            } else {
                (l * 10i128.pow(diff as u32))
                    .checked_mul(*r)
                    .expect("Mayor overflow for multiplication")
            };

            let res = res / 10i128.pow(res_s as u32);

            // The precision of the resulting array will change if one of the
            // multiplications during the iteration produces a value bigger
            // than the possible value for the initial precision

            //  10.0000 -> 6, 4
            //  10.0000 -> 6, 4
            // -----------------
            // 100.0000 -> 7, 4
            if res.abs() > max_value(res_p) {
                res_p = number_digits(res);
            }

            result.push(res);
        }

        let validity = combine_validities(lhs.validity(), rhs.validity());
        let values = Buffer::from(result);

        Ok(PrimitiveArray::<i128>::from_data(
            DataType::Decimal(res_p, res_s),
            values,
            validity,
        ))
    } else {
        Err(ArrowError::InvalidArgumentError(
            "Incorrect data type for the array".to_string(),
        ))
    }
}
