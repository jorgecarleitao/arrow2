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

//! Defines basic arithmetic kernels for `PrimitiveArrays`.
//use std::ops::{Add, Div, Mul, Neg, Sub};

//use crate::types::NativeType;
use crate::array::{Array, PrimitiveArray};
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result},
};

use crate::compute::arity::binary;

/// Maximum value that can exist with a selected precision
#[inline]
fn max_value(precision: usize) -> i128 {
    10i128.pow(precision as u32) - 1
}

/// Adds two decimal primitive arrays with the same precision and scale
/// If the precision and scale is different, then an InvalidArgumentError is returned
#[inline]
pub fn add(lhs: &PrimitiveArray<i128>, rhs: &PrimitiveArray<i128>) -> Result<PrimitiveArray<i128>> {
    // Matching on both data types from both arrays
    // This match will be true only when precision and scale from both
    // arrays are the same, otherwise it will return and ArrowError
    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Decimal(lhs_p, lhs_s), DataType::Decimal(rhs_p, rhs_s))
            if (lhs_p == rhs_p && lhs_s == rhs_s) =>
        {
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
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(
                "Arrays must have the same precision and scale".to_string(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Primitive;
    use crate::datatypes::DataType;

    #[test]
    fn test_max_value() {
        let res = max_value(5);
        assert_eq!(res, 999);
    }

    #[test]
    fn test_add_decimal() {
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
}
