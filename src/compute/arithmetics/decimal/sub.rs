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

//! Defines the subtract arithmetic kernels for Decimal `PrimitiveArrays`.

use crate::{
    array::{Array, PrimitiveArray},
    buffer::Buffer,
    compute::{
        arity::{binary, binary_checked},
        utils::combine_validities,
    },
    datatypes::DataType,
    error::{ArrowError, Result},
};

use super::{adjusted_precision_scale, max_value, number_digits};

/// Subtract two decimal primitive arrays with the same precision and scale. If
/// the precision and scale is different, then an InvalidArgumentError is
/// returned. This function panics if the subtracted numbers result in a number
/// smaller than the possible number for the selected precision.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::sub::sub;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(1i128), Some(1i128), None, Some(2i128)]).to(DataType::Decimal(5, 2));
/// let b = Primitive::from(&vec![Some(1i128), Some(2i128), None, Some(2i128)]).to(DataType::Decimal(5, 2));
///
/// let result = sub(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(0i128), Some(-1i128), None, Some(0i128)]).to(DataType::Decimal(5, 2));
///
/// assert_eq!(result, expected);
/// ```
pub fn sub(lhs: &PrimitiveArray<i128>, rhs: &PrimitiveArray<i128>) -> Result<PrimitiveArray<i128>> {
    // Matching on both data types from both arrays This match will be true
    // only when precision and scale from both arrays are the same, otherwise
    // it will return and ArrowError
    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s)) => {
            if lhs_p == rhs_p && lhs_s == rhs_s {
                // Closure for the binary operation. This closure will panic if
                // the sum of the values is larger than the max value possible
                // for the decimal precision
                let op = move |a, b| {
                    let res: i128 = a - b;

                    if res.abs() > max_value(*lhs_p) {
                        panic!("Overflow in subtract presented for precision {}", lhs_p);
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

/// Saturated subtraction of two decimal primitive arrays with the same
/// precision and scale. If the precision and scale is different, then an
/// InvalidArgumentError is returned. If the result from the sum is smaller
/// than the possible number with the selected precision then the resulted
/// number in the arrow array is the minimum number for the selected precision.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::sub::saturating_sub;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(-99000i128), Some(11100i128), None, Some(22200i128)]).to(DataType::Decimal(5, 2));
/// let b = Primitive::from(&vec![Some(01000i128), Some(22200i128), None, Some(11100i128)]).to(DataType::Decimal(5, 2));
///
/// let result = saturating_sub(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(-99999i128), Some(-11100i128), None, Some(11100i128)]).to(DataType::Decimal(5, 2));
///
/// assert_eq!(result, expected);
/// ```
pub fn saturating_sub(
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
                let op = move |a, b| {
                    let res: i128 = a - b;
                    let max: i128 = max_value(*lhs_p);

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

/// Checked subtract of two decimal primitive arrays with the same precision
/// and scale. If the precision and scale is different, then an
/// InvalidArgumentError is returned. If the result from the sub is larger than
/// the possible number with the selected precision (overflowing), then the
/// validity for that index is changed to None
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::sub::checked_sub;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(-99000i128), Some(11100i128), None, Some(22200i128)]).to(DataType::Decimal(5, 2));
/// let b = Primitive::from(&vec![Some(01000i128), Some(22200i128), None, Some(11100i128)]).to(DataType::Decimal(5, 2));
///
/// let result = checked_sub(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![None, Some(-11100i128), None, Some(11100i128)]).to(DataType::Decimal(5, 2));
///
/// assert_eq!(result, expected);
/// ```
pub fn checked_sub(
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
                let op = move |a, b| {
                    let res: i128 = a - b;

                    match res {
                        res if res.abs() > max_value(*lhs_p) => None,
                        _ => Some(res),
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

/// Adaptive subtract of two decimal primitive arrays with different precision
/// and scale. If the precision and scale is different, then the smallest scale
/// and precision is adjusted to the largest precision and scale. If during the
/// addition one of the results is smaller than the min possible value, the
/// result precision is changed to the precision of the min value
///
/// ```nocode
///  99.9999 -> 6, 4
/// -00.0001 -> 6, 4
/// -----------------
/// 100.0000 -> 7, 4
/// ```
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::decimal::sub::adaptive_sub;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::DataType;
///
/// let a = Primitive::from(&vec![Some(99_9999i128)]).to(DataType::Decimal(6, 4));
/// let b = Primitive::from(&vec![Some(-00_0001i128)]).to(DataType::Decimal(6, 4));
/// let result = adaptive_sub(&a, &b).unwrap();
/// let expected = Primitive::from(&vec![Some(100_0000i128)]).to(DataType::Decimal(7, 4));
///
/// assert_eq!(result, expected);
/// ```
pub fn adaptive_sub(
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
            // Based on the array's scales one of the arguments in the sum has to be shifted
            // to the left to match the final scale
            let res: i128 = if lhs_s > rhs_s {
                l - r * 10i128.pow(diff as u32)
            } else {
                l * 10i128.pow(diff as u32) - r
            };

            // The precision of the resulting array will change if one of the
            // subtraction during the iteration produces a value bigger than the
            // possible value for the initial precision

            //  -99.9999 -> 6, 4
            //   00.0001 -> 6, 4
            // -----------------
            // -100.0000 -> 7, 4
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
    fn test_subtract_normal() {
        let a = Primitive::from(&vec![
            Some(11111i128),
            Some(22200i128),
            None,
            Some(40000i128),
        ])
        .to(DataType::Decimal(5, 2));

        let b = Primitive::from(&vec![
            Some(22222i128),
            Some(11100i128),
            None,
            Some(11100i128),
        ])
        .to(DataType::Decimal(5, 2));

        let result = sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![
            Some(-11111i128),
            Some(11100i128),
            None,
            Some(28900i128),
        ])
        .to(DataType::Decimal(5, 2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtract_decimal_wrong_precision() {
        let a = Primitive::from(&vec![None]).to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![None]).to(DataType::Decimal(6, 2));
        let result = sub(&a, &b);

        if result.is_ok() {
            panic!("Should panic for different precision");
        }
    }

    #[test]
    #[should_panic(expected = "Overflow in subtract presented for precision 5")]
    fn test_subtract_panic() {
        let a = Primitive::from(&vec![Some(-99999i128)]).to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![Some(1i128)]).to(DataType::Decimal(5, 2));
        let _ = sub(&a, &b);
    }

    #[test]
    fn test_subtract_saturating() {
        let a = Primitive::from(&vec![
            Some(11111i128),
            Some(22200i128),
            None,
            Some(40000i128),
        ])
        .to(DataType::Decimal(5, 2));

        let b = Primitive::from(&vec![
            Some(22222i128),
            Some(11100i128),
            None,
            Some(11100i128),
        ])
        .to(DataType::Decimal(5, 2));

        let result = saturating_sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![
            Some(-11111i128),
            Some(11100i128),
            None,
            Some(28900i128),
        ])
        .to(DataType::Decimal(5, 2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtract_saturating_overflow() {
        let a = Primitive::from(&vec![
            Some(-99999i128),
            Some(-99999i128),
            Some(-99999i128),
            Some(99999i128),
        ])
        .to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![
            Some(00001i128),
            Some(00100i128),
            Some(10000i128),
            Some(-99999i128),
        ])
        .to(DataType::Decimal(5, 2));

        let result = saturating_sub(&a, &b).unwrap();

        let expected = Primitive::from(&vec![
            Some(-99999i128),
            Some(-99999i128),
            Some(-99999i128),
            Some(99999i128),
        ])
        .to(DataType::Decimal(5, 2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtract_checked() {
        let a = Primitive::from(&vec![
            Some(11111i128),
            Some(22200i128),
            None,
            Some(40000i128),
        ])
        .to(DataType::Decimal(5, 2));

        let b = Primitive::from(&vec![
            Some(22222i128),
            Some(11100i128),
            None,
            Some(11100i128),
        ])
        .to(DataType::Decimal(5, 2));

        let result = checked_sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![
            Some(-11111i128),
            Some(11100i128),
            None,
            Some(28900i128),
        ])
        .to(DataType::Decimal(5, 2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtract_checked_overflow() {
        let a = Primitive::from(&vec![Some(4i128), Some(-99999i128)]).to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![Some(2i128), Some(1i128)]).to(DataType::Decimal(5, 2));
        let result = checked_sub(&a, &b).unwrap();
        let expected = Primitive::from(&vec![Some(2i128), None]).to(DataType::Decimal(5, 2));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtract_adaptive() {
        //     11.1111 -> 6, 4
        //  11111.11   -> 7, 2
        // ------------------
        // -11099.9989 -> 9, 4
        let a = Primitive::from(&vec![Some(11_1111i128)]).to(DataType::Decimal(6, 4));
        let b = Primitive::from(&vec![Some(11111_11i128)]).to(DataType::Decimal(7, 2));
        let result = adaptive_sub(&a, &b).unwrap();

        let expected = Primitive::from(&vec![Some(-11099_9989i128)]).to(DataType::Decimal(9, 4));

        assert_eq!(result, expected);
        assert_eq!(result.data_type(), &DataType::Decimal(9, 4));

        // 11111.0    -> 6, 1
        //     0.1111 -> 5, 4
        // -----------------
        // 11110.8889 -> 9, 4
        let a = Primitive::from(&vec![Some(11111_0i128)]).to(DataType::Decimal(6, 1));
        let b = Primitive::from(&vec![Some(1111i128)]).to(DataType::Decimal(5, 4));
        let result = adaptive_sub(&a, &b).unwrap();

        let expected = Primitive::from(&vec![Some(11110_8889i128)]).to(DataType::Decimal(9, 4));

        assert_eq!(result, expected);
        assert_eq!(result.data_type(), &DataType::Decimal(9, 4));

        //  11111.11   -> 7, 2
        //  11111.111  -> 8, 3
        // -----------------
        // -00000.001  -> 8, 3
        let a = Primitive::from(&vec![Some(11111_11i128)]).to(DataType::Decimal(7, 2));
        let b = Primitive::from(&vec![Some(11111_111i128)]).to(DataType::Decimal(8, 3));
        let result = adaptive_sub(&a, &b).unwrap();

        let expected = Primitive::from(&vec![Some(-00000_001i128)]).to(DataType::Decimal(8, 3));

        assert_eq!(result, expected);
        assert_eq!(result.data_type(), &DataType::Decimal(8, 3));

        //  99.9999 -> 6, 4
        // -00.0001 -> 6, 4
        // -----------------
        // 100.0000 -> 7, 4
        let a = Primitive::from(&vec![Some(99_9999i128)]).to(DataType::Decimal(6, 4));
        let b = Primitive::from(&vec![Some(-00_0001i128)]).to(DataType::Decimal(6, 4));
        let result = adaptive_sub(&a, &b).unwrap();

        let expected = Primitive::from(&vec![Some(100_0000i128)]).to(DataType::Decimal(7, 4));

        assert_eq!(result, expected);
        assert_eq!(result.data_type(), &DataType::Decimal(7, 4));
    }
}
