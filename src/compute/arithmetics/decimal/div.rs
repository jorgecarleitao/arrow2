// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines the division arithmetic kernels for Decimal
//! `PrimitiveArrays`.

use crate::{
    array::{Array, PrimitiveArray},
    buffer::Buffer,
    compute::{
        arity::{binary, binary_checked},
        utils::combine_validities,
    },
};
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::{adjusted_precision_scale, max_value, number_digits};

/// Divide two decimal primitive arrays with the same precision and scale. If
/// the precision and scale is different, then an InvalidArgumentError is
/// returned. This function panics if the dividend is divided by 0 or None.
/// This function also panics if the division produces a number larger
/// than the possible number for the array precision.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::div::div;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(1_00i128), Some(4_00i128), Some(6_00i128)]).to(DataType::Decimal(5, 2));
/// let b = Primitive::from(&vec![Some(1_00i128), Some(2_00i128), Some(2_00i128)]).to(DataType::Decimal(5, 2));
///
/// let result = div(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(1_00i128), Some(2_00i128), Some(3_00i128)]).to(DataType::Decimal(5, 2));
///
/// assert_eq!(result, expected);
/// ```
pub fn div(lhs: &PrimitiveArray<i128>, rhs: &PrimitiveArray<i128>) -> Result<PrimitiveArray<i128>> {
    // Matching on both data types from both arrays
    // This match will be true only when precision and scale from both
    // arrays are the same, otherwise it will return and ArrowError
    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s)) => {
            if lhs_p == rhs_p && lhs_s == rhs_s {
                // Closure for the binary operation. This closure will panic if
                // the sum of the values is larger than the max value possible
                // for the decimal precision
                let op = move |a: i128, b: i128| {
                    // The division is done using the numbers without scale.
                    // The dividend is scaled up to maintain precision after the
                    // division

                    //   222.222 -->  222222000
                    //   123.456 -->     123456
                    // --------       ---------
                    //     1.800 <--       1800
                    let numeral: i128 = a * 10i128.pow(*lhs_s as u32);

                    // The division can overflow if the dividend is divided
                    // by zero.
                    let res: i128 = numeral.checked_div(b).expect("Found division by zero");

                    if res.abs() > max_value(*lhs_p) {
                        panic!(
                            "Overflow in multiplication presented for precision {}",
                            lhs_p
                        );
                    }

                    res
                };

                binary(lhs, rhs, lhs.data_type().clone(), op)
            } else {
                return Err(ArrowError::InvalidArgumentError(
                    "Arrays must have the same precision and scale".to_string(),
                ));
            }
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(
                "Incorrect data type for the array".to_string(),
            ));
        }
    }
}

/// Saturated division of two decimal primitive arrays with the same
/// precision and scale. If the precision and scale is different, then an
/// InvalidArgumentError is returned. If the result from the division is
/// larger than the possible number with the selected precision then the
/// resulted number in the arrow array is the maximum number for the selected
/// precision. The function panics if divided by zero.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::div::saturating_div;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(999_99i128), Some(4_00i128), Some(6_00i128)]).to(DataType::Decimal(5, 2));
/// let b = Primitive::from(&vec![Some(000_01i128), Some(2_00i128), Some(2_00i128)]).to(DataType::Decimal(5, 2));
///
/// let result = saturating_div(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(999_99i128), Some(2_00i128), Some(3_00i128)]).to(DataType::Decimal(5, 2));
///
/// assert_eq!(result, expected);
/// ```
pub fn saturating_div(
    lhs: &PrimitiveArray<i128>,
    rhs: &PrimitiveArray<i128>,
) -> Result<PrimitiveArray<i128>> {
    // Matching on both data types from both arrays. This match will be true
    // only when precision and scale from both arrays are the same, otherwise
    // it will return and ArrowError
    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s)) => {
            if lhs_p == rhs_p && lhs_s == rhs_s {
                // Closure for the binary operation.
                let op = move |a: i128, b: i128| {
                    let numeral: i128 = a * 10i128.pow(*lhs_s as u32);

                    match numeral.checked_div(b) {
                        Some(res) => {
                            let max = max_value(*lhs_p);

                            match res {
                                res if res.abs() > max => {
                                    if res > 0 {
                                        max
                                    } else {
                                        -1 * max
                                    }
                                }
                                _ => res,
                            }
                        }
                        None => 0,
                    }
                };

                binary(lhs, rhs, lhs.data_type().clone(), op)
            } else {
                return Err(ArrowError::InvalidArgumentError(
                    "Arrays must have the same precision and scale".to_string(),
                ));
            }
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(
                "Incorrect data type for the array".to_string(),
            ));
        }
    }
}

/// Checked division of two decimal primitive arrays with the same precision
/// and scale. If the precision and scale is different, then an
/// InvalidArgumentError is returned. If the divisor is zero, then the
/// validity for that index is changed to None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::div::checked_div;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(1_00i128), Some(4_00i128), Some(6_00i128)]).to(DataType::Decimal(5, 2));
/// let b = Primitive::from(&vec![Some(000_00i128), None, Some(2_00i128)]).to(DataType::Decimal(5, 2));
///
/// let result = checked_div(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![None, None, Some(3_00i128)]).to(DataType::Decimal(5, 2));
///
/// assert_eq!(result, expected);
/// ```
pub fn checked_div(
    lhs: &PrimitiveArray<i128>,
    rhs: &PrimitiveArray<i128>,
) -> Result<PrimitiveArray<i128>> {
    // Matching on both data types from both arrays. This match will be true
    // only when precision and scale from both arrays are the same, otherwise
    // it will return and ArrowError
    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s)) => {
            if lhs_p == rhs_p && lhs_s == rhs_s {
                // Closure for the binary operation.
                let op = move |a: i128, b: i128| {
                    let numeral: i128 = a * 10i128.pow(*lhs_s as u32);

                    match numeral.checked_div(b) {
                        Some(res) => match res {
                            res if res.abs() > max_value(*lhs_p) => None,
                            _ => Some(res),
                        },
                        None => None,
                    }
                };

                binary_checked(lhs, rhs, lhs.data_type().clone(), op)
            } else {
                return Err(ArrowError::InvalidArgumentError(
                    "Arrays must have the same precision and scale".to_string(),
                ));
            }
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(
                "Incorrect data type for the array".to_string(),
            ));
        }
    }
}

/// Adaptive division of two decimal primitive arrays with different precision
/// and scale. If the precision and scale is different, then the smallest scale
/// and precision is adjusted to the largest precision and scale. If during the
/// division one of the results is larger than the max possible value, the
/// result precision is changed to the precision of the max value. The function
/// panics when divided by zero.
///
/// ```nocode
///  1000.00   -> 7, 2
///    10.0000 -> 6, 4
/// -----------------
///   100.0000 -> 9, 4
/// ```
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::div::adaptive_div;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(1000_00i128)]).to(DataType::Decimal(7, 2));
/// let b = Primitive::from(&vec![Some(10_0000i128)]).to(DataType::Decimal(6, 4));
/// let result = adaptive_div(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(100_0000i128)]).to(DataType::Decimal(9, 4));
///
/// assert_eq!(result, expected);
/// ```
pub fn adaptive_div(
    lhs: &PrimitiveArray<i128>,
    rhs: &PrimitiveArray<i128>,
) -> Result<PrimitiveArray<i128>> {
    // Checking if both arrays have the same length
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Arrays must have the same length".to_string(),
        ));
    }

    if let (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s)) =
        (lhs.data_type(), rhs.data_type())
    {
        // The resulting precision is mutable because it could change while
        // looping through the iterator
        let (mut res_p, res_s, diff) = adjusted_precision_scale(*lhs_p, *lhs_s, *rhs_p, *rhs_s);

        let mut result = Vec::new();
        for (l, r) in lhs.values().iter().zip(rhs.values().iter()) {
            let numeral: i128 = l * 10i128.pow(res_s as u32);

            // Based on the array's scales one of the arguments in the sum has to be shifted
            // to the left to match the final scale
            let res = if lhs_s > rhs_s {
                numeral
                    .checked_div(r * 10i128.pow(diff as u32))
                    .expect("Found division by zero")
            } else {
                (numeral * 10i128.pow(diff as u32))
                    .checked_div(*r)
                    .expect("Found division by zero")
            };

            // The precision of the resulting array will change if one of the
            // multiplications during the iteration produces a value bigger
            // than the possible value for the initial precision

            //  10.0000 -> 6, 4
            //  00.1000 -> 6, 4
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
        return Err(ArrowError::InvalidArgumentError(
            "Incorrect data type for the array".to_string(),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Primitive;
    use crate::datatypes::DataType;

    #[test]
    fn test_divide_normal() {
        //   222.222 -->  222222000
        //   123.456 -->     123456
        // --------       ---------
        //     1.800 <--       1800
        let a = Primitive::from(&vec![
            Some(222_222i128),
            Some(10_000i128),
            Some(20_000i128),
            None,
            Some(30_000i128),
            Some(123_456i128),
        ])
        .to(DataType::Decimal(7, 3));

        let b = Primitive::from(&vec![
            Some(123_456i128),
            Some(2_000i128),
            Some(3_000i128),
            Some(4_000i128),
            Some(4_000i128),
            Some(654_321i128),
        ])
        .to(DataType::Decimal(7, 3));

        let result = div(&a, &b).unwrap();
        let expected = Primitive::from(&vec![
            Some(1_800i128),
            Some(5_000i128),
            Some(6_666i128),
            None,
            Some(7_500i128),
            Some(0_188i128),
        ])
        .to(DataType::Decimal(7, 3));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_divide_decimal_wrong_precision() {
        let a = Primitive::from(&vec![None]).to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![None]).to(DataType::Decimal(6, 2));
        let result = div(&a, &b);

        if result.is_ok() {
            panic!("Should panic for different precision");
        }
    }

    #[test]
    #[should_panic(expected = "Overflow in multiplication presented for precision 5")]
    fn test_divide_panic() {
        let a = Primitive::from(&vec![Some(99999i128)]).to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![Some(000_01i128)]).to(DataType::Decimal(5, 2));
        let _ = div(&a, &b);
    }

    #[test]
    fn test_divide_saturating() {
        let a = Primitive::from(&vec![
            Some(222_222i128),
            Some(10_000i128),
            Some(20_000i128),
            None,
            Some(30_000i128),
            Some(123_456i128),
        ])
        .to(DataType::Decimal(7, 3));

        let b = Primitive::from(&vec![
            Some(123_456i128),
            Some(2_000i128),
            Some(3_000i128),
            Some(4_000i128),
            Some(4_000i128),
            Some(654_321i128),
        ])
        .to(DataType::Decimal(7, 3));

        let result = saturating_div(&a, &b).unwrap();
        let expected = Primitive::from(&vec![
            Some(1_800i128),
            Some(5_000i128),
            Some(6_666i128),
            None,
            Some(7_500i128),
            Some(0_188i128),
        ])
        .to(DataType::Decimal(7, 3));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_divide_saturating_overflow() {
        let a = Primitive::from(&vec![
            Some(99999i128),
            Some(99999i128),
            Some(99999i128),
            Some(99999i128),
            Some(99999i128),
        ])
        .to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![
            Some(-00001i128),
            Some(00001i128),
            Some(00010i128),
            Some(-00020i128),
            Some(00000i128),
        ])
        .to(DataType::Decimal(5, 2));

        let result = saturating_div(&a, &b).unwrap();

        let expected = Primitive::from(&vec![
            Some(-99999i128),
            Some(99999i128),
            Some(99999i128),
            Some(-99999i128),
            Some(00000i128),
        ])
        .to(DataType::Decimal(5, 2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_divide_checked() {
        let a = Primitive::from(&vec![
            Some(222_222i128),
            Some(10_000i128),
            Some(20_000i128),
            None,
            Some(30_000i128),
            Some(123_456i128),
        ])
        .to(DataType::Decimal(7, 3));

        let b = Primitive::from(&vec![
            Some(123_456i128),
            Some(2_000i128),
            Some(3_000i128),
            Some(4_000i128),
            Some(4_000i128),
            Some(654_321i128),
        ])
        .to(DataType::Decimal(7, 3));

        let result = div(&a, &b).unwrap();
        let expected = Primitive::from(&vec![
            Some(1_800i128),
            Some(5_000i128),
            Some(6_666i128),
            None,
            Some(7_500i128),
            Some(0_188i128),
        ])
        .to(DataType::Decimal(7, 3));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_divide_checked_overflow() {
        let a = Primitive::from(&vec![Some(1_00i128), Some(4_00i128), Some(6_00i128)])
            .to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![Some(000_00i128), None, Some(2_00i128)])
            .to(DataType::Decimal(5, 2));

        let result = checked_div(&a, &b).unwrap();
        let expected =
            Primitive::from(&vec![None, None, Some(3_00i128)]).to(DataType::Decimal(5, 2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_divide_adaptive() {
        //  1000.00   -> 7, 2
        //    10.0000 -> 6, 4
        // -----------------
        //   100.0000 -> 9, 4
        let a = Primitive::from(&vec![Some(1000_00i128)]).to(DataType::Decimal(7, 2));
        let b = Primitive::from(&vec![Some(10_0000i128)]).to(DataType::Decimal(6, 4));
        let result = adaptive_div(&a, &b).unwrap();

        let expected = Primitive::from(&vec![Some(100_0000i128)]).to(DataType::Decimal(9, 4));

        assert_eq!(result, expected);
        assert_eq!(result.data_type(), &DataType::Decimal(9, 4));

        //   11111.0    -> 6, 1
        //      10.002  -> 5, 3
        // -----------------
        //    1110.877  -> 8, 3
        let a = Primitive::from(&vec![Some(11111_0i128)]).to(DataType::Decimal(6, 1));
        let b = Primitive::from(&vec![Some(10_002i128)]).to(DataType::Decimal(5, 3));
        let result = adaptive_div(&a, &b).unwrap();

        let expected = Primitive::from(&vec![Some(1110_877i128)]).to(DataType::Decimal(8, 3));

        assert_eq!(result, expected);
        assert_eq!(result.data_type(), &DataType::Decimal(8, 3));

        //     12345.67   ->  7, 2
        //     12345.678  ->  8, 3
        // -----------------
        //         0.999  ->  8, 3
        let a = Primitive::from(&vec![Some(12345_67i128)]).to(DataType::Decimal(7, 2));
        let b = Primitive::from(&vec![Some(12345_678i128)]).to(DataType::Decimal(8, 3));
        let result = adaptive_div(&a, &b).unwrap();

        let expected = Primitive::from(&vec![Some(0_999i128)]).to(DataType::Decimal(8, 3));

        assert_eq!(result, expected);
        assert_eq!(result.data_type(), &DataType::Decimal(8, 3));
    }
}
