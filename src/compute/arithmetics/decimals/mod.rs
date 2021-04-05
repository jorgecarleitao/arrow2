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

//! Defines the arithmetic kernels for Decimal `PrimitiveArrays`.
//! The Decimal type specifies the precision and scale parameters. These
//! affect the arithmetic operations and need to be considered while
//! doing operations with Decimal numbers.

pub mod add;
pub mod subtract;

/// Maximum value that can exist with a selected precision
#[inline]
fn max_value(precision: usize) -> i128 {
    10i128.pow(precision as u32) - 1
}

// Calculates the number of digits in a i128 number
fn number_digits(num: i128) -> usize {
    let mut num = num.abs();
    let mut digit: i128 = 0;
    let base = 10i128;

    while num != 0 {
        num /= base;
        digit += 1;
    }

    digit as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_value() {
        assert_eq!(999, max_value(3));
        assert_eq!(99999, max_value(5));
        assert_eq!(999999, max_value(6));
    }

    #[test]
    fn test_number_digits() {
        assert_eq!(2, number_digits(12i128));
        assert_eq!(3, number_digits(123i128));
        assert_eq!(4, number_digits(1234i128));
        assert_eq!(6, number_digits(123456i128));
        assert_eq!(7, number_digits(1234567i128));
        assert_eq!(7, number_digits(-1234567i128));
        assert_eq!(3, number_digits(-123i128));
    }
}
