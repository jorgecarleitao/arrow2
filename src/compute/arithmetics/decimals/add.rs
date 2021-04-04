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

//! Defines the addition arithmetic kernels for Decimal `PrimitiveArrays`.
//! The Decimal type specifies the precision and scale parameters. These
//! affect the arithmetic operations and need to be considered while
//! doing operations with Decimal numbers.

//use std::ops::{Add, Div, Mul, Neg, Sub};

//use crate::types::NativeType;
use crate::array::{Array, PrimitiveArray};
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result},
};

use crate::compute::arity::{binary, binary_with_change, try_binary};

/// Maximum value that can exist with a selected precision
#[inline]
fn max_value(precision: usize) -> i128 {
    10i128.pow(precision as u32) - 1
}

// Calculates the number of digits in a i128 number
//fn number_digits(num: i128) -> usize {
//    let mut num = num.abs();
//    let mut digit: i128 = 0;
//    let base = 10i128;
//
//    while num != 0 {
//        num /= base;
//        digit += 1;
//    }
//
//    digit as usize
//}

/// Adds two decimal primitive arrays with the same precision and scale
/// If the precision and scale is different, then an InvalidArgumentError is returned.
/// This function panics if the added numbers result in a number larger than the possible
/// number for the selected precision.
pub fn add(lhs: &PrimitiveArray<i128>, rhs: &PrimitiveArray<i128>) -> Result<PrimitiveArray<i128>> {
    // Matching on both data types from both arrays
    // This match will be true only when precision and scale from both
    // arrays are the same, otherwise it will return and ArrowError
    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s)) => {
            if lhs_p == rhs_p && lhs_s == rhs_s {
                // Closure for the binary operation.
                // This closure will panic if the sum of the values is larger
                // than the max value possible for the decimal precision
                let op = move |a, b| {
                    let res = a + b;

                    if res > max_value(*lhs_p) {
                        panic!("Max value presented for this precision");
                    }

                    res
                };

                binary(lhs, rhs, op)
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

/// Saturated addition of two decimal primitive arrays with the same precision and scale.
/// If the precision and scale is different, then an InvalidArgumentError is returned.
/// If the result from the sum is larger than the possible number with the selected
/// precision then the resulted number in the arrow array is the maximum number for
/// the selected precision.
pub fn saturating_add(
    lhs: &PrimitiveArray<i128>,
    rhs: &PrimitiveArray<i128>,
) -> Result<PrimitiveArray<i128>> {
    // Matching on both data types from both arrays
    // This match will be true only when precision and scale from both
    // arrays are the same, otherwise it will return and ArrowError
    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s)) => {
            if lhs_p == rhs_p && lhs_s == rhs_s {
                // Closure for the binary operation.
                // This closure will panic if the sum of the values is larger
                // than the max value possible for the decimal precision
                let op = move |a, b| {
                    let res = a + b;
                    let max = max_value(*lhs_p);

                    match res {
                        res if res > max => max,
                        _ => res,
                    }
                };

                binary(lhs, rhs, op)
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

/// Checked addition of two decimal primitive arrays with the same precision and scale.
/// If the precision and scale is different, then an InvalidArgumentError is returned.
/// If the result from the sum is larger than the possible number with the selected
/// precision then the function returns an ArrowError::ArithmeticError.
pub fn checked_add(
    lhs: &PrimitiveArray<i128>,
    rhs: &PrimitiveArray<i128>,
) -> Result<PrimitiveArray<i128>> {
    // Matching on both data types from both arrays
    // This match will be true only when precision and scale from both
    // arrays are the same, otherwise it will return and ArrowError
    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s)) => {
            if lhs_p == rhs_p && lhs_s == rhs_s {
                // Closure for the binary operation.
                // This closure will panic if the sum of the values is larger
                // than the max value possible for the decimal precision
                let op = move |a, b| {
                    let res = a + b;
                    let max = max_value(*lhs_p);

                    match res {
                        res if res > max => {
                            return Err(ArrowError::ArithmeticError(
                                "Saturated result in addition".to_string(),
                            ));
                        }
                        _ => Ok(res),
                    }
                };

                try_binary(lhs, rhs, op)
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

/// Adaptive addition of two decimal primitive arrays with different precision and scale.
/// If the precision and scale is different, then the smallest scale and precision is
/// adjusted to the largest precision and scale. If during the addition one of the results
/// is larger than the max possible value, the result precision is changed to the precision
/// of the max value
pub fn adaptive_add(
    lhs: &PrimitiveArray<i128>,
    rhs: &PrimitiveArray<i128>,
) -> Result<PrimitiveArray<i128>> {
    if let (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s)) =
        (lhs.data_type(), rhs.data_type())
    {
        //    11.1111 -> 5, 4
        // 11111.01   -> 7, 2
        // -----------------
        // 11122.1211 -> 9, 4
        //
        //
        // The scale for the resulting vector will be the largest one from both arrays
        // The diff variable is the difference between the scales and it is used to calculate
        // the final precision
        let (res_s, diff) = if lhs_s > rhs_s {
            (*lhs_s, lhs_s - rhs_s)
        } else {
            (*rhs_s, rhs_s - lhs_s)
        };

        // The final precision is the largest precision plus the difference between the scales
        let res_p = (if lhs_p > rhs_p { lhs_p } else { rhs_p }) + diff;

        // Closure for the binary operation.
        // This closure will panic if the sum of the values is larger
        // than the max value possible for the decimal precision
        let op = move |a, b| {
            // Based on the array's scales one of the arguments in the sum has to be shifted
            // to the left to match the final scale
            let res = if lhs_s > rhs_s {
                a + b * 10i128.pow(diff as u32)
            } else {
                b + a * 10i128.pow(diff as u32)
            };

            let max = max_value(res_p);
            if res > max {
                panic!("Max value presented for this precision");
            }

            res
        };

        binary_with_change(lhs, rhs, DataType::Decimal(res_p, res_s), op)
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
    fn test_max_value() {
        assert_eq!(999, max_value(3));
        assert_eq!(99999, max_value(5));
        assert_eq!(999999, max_value(6));
    }

    #[test]
    fn test_add_normal() {
        let a = Primitive::from(&vec![
            Some(11111i128),
            Some(11100i128),
            None,
            Some(22200i128),
        ])
        .to(DataType::Decimal(5, 2));

        let b = Primitive::from(&vec![
            Some(22222i128),
            Some(22200i128),
            None,
            Some(11100i128),
        ])
        .to(DataType::Decimal(5, 2));

        let result = add(&a, &b).unwrap();
        let expected = Primitive::from(&vec![
            Some(33333i128),
            Some(33300i128),
            None,
            Some(33300i128),
        ])
        .to(DataType::Decimal(5, 2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_decimal_wrong_precision() {
        let a = Primitive::from(&vec![None]).to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![None]).to(DataType::Decimal(6, 2));
        let result = add(&a, &b);

        if result.is_ok() {
            panic!("Should panic for different precision");
        }
    }

    #[test]
    #[should_panic(expected = "Max value presented for this precision")]
    fn test_add_panic() {
        let a = Primitive::from(&vec![Some(99999i128)]).to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![Some(1i128)]).to(DataType::Decimal(5, 2));
        let _ = add(&a, &b);
    }

    #[test]
    fn test_add_saturating() {
        let a = Primitive::from(&vec![
            Some(11111i128),
            Some(11100i128),
            None,
            Some(22200i128),
        ])
        .to(DataType::Decimal(5, 2));

        let b = Primitive::from(&vec![
            Some(22222i128),
            Some(22200i128),
            None,
            Some(11100i128),
        ])
        .to(DataType::Decimal(5, 2));

        let result = saturating_add(&a, &b).unwrap();
        let expected = Primitive::from(&vec![
            Some(33333i128),
            Some(33300i128),
            None,
            Some(33300i128),
        ])
        .to(DataType::Decimal(5, 2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_saturating_overflow() {
        let a = Primitive::from(&vec![
            Some(99999i128),
            Some(99999i128),
            Some(99999i128),
            Some(99999i128),
        ])
        .to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![
            Some(00001i128),
            Some(00100i128),
            Some(10000i128),
            Some(99999i128),
        ])
        .to(DataType::Decimal(5, 2));

        let result = saturating_add(&a, &b).unwrap();

        let expected = Primitive::from(&vec![
            Some(99999i128),
            Some(99999i128),
            Some(99999i128),
            Some(99999i128),
        ])
        .to(DataType::Decimal(5, 2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_checked() {
        let a = Primitive::from(&vec![
            Some(11111i128),
            Some(11100i128),
            None,
            Some(22200i128),
        ])
        .to(DataType::Decimal(5, 2));

        let b = Primitive::from(&vec![
            Some(22222i128),
            Some(22200i128),
            None,
            Some(11100i128),
        ])
        .to(DataType::Decimal(5, 2));

        let result = checked_add(&a, &b).unwrap();
        let expected = Primitive::from(&vec![
            Some(33333i128),
            Some(33300i128),
            None,
            Some(33300i128),
        ])
        .to(DataType::Decimal(5, 2));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_checked_overflow() {
        let a = Primitive::from(&vec![Some(99999i128)]).to(DataType::Decimal(5, 2));
        let b = Primitive::from(&vec![Some(1i128)]).to(DataType::Decimal(5, 2));
        let result = checked_add(&a, &b);

        match result {
            Err(err) => match err {
                ArrowError::ArithmeticError(_) => assert!(true),
                _ => panic!("Should return ArrowError::ArithmeticError should be detected"),
            },
            _ => panic!("Should return ArrowError::ArithmeticError should be detected"),
        }
    }

    //#[test]
    //fn test_number_digits() {
    //    assert_eq!(2, number_digits(12i128));
    //    assert_eq!(3, number_digits(123i128));
    //    assert_eq!(4, number_digits(1234i128));
    //    assert_eq!(6, number_digits(123456i128));
    //    assert_eq!(7, number_digits(1234567i128));
    //    assert_eq!(7, number_digits(-1234567i128));
    //    assert_eq!(3, number_digits(-123i128));
    //}

    #[test]
    fn test_add_adaptive() {
        //    11.1111 -> 6, 4
        // 11111.11   -> 7, 2
        // -----------------
        // 11122.2211 -> 9, 4
        let a = Primitive::from(&vec![Some(111111i128)]).to(DataType::Decimal(6, 4));
        let b = Primitive::from(&vec![Some(1111111i128)]).to(DataType::Decimal(7, 2));
        let result = adaptive_add(&a, &b).unwrap();

        let expected = Primitive::from(&vec![Some(111222211i128)]).to(DataType::Decimal(9, 4));

        assert_eq!(result, expected);
        assert_eq!(result.data_type(), &DataType::Decimal(9, 4));
    }
}
