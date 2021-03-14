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

use std::collections::HashMap;

use regex::Regex;

use super::utils::{combine_validities, unary_utf8_boolean};
use crate::array::{BooleanArray, Offset, Utf8Array};
use crate::error::{ArrowError, Result};
use crate::{array::*, bitmap::Bitmap};

pub fn regex_match<O: Offset>(values: &Utf8Array<O>, regex: &Utf8Array<O>) -> Result<BooleanArray> {
    if values.len() != regex.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let mut map = HashMap::new();
    let validity = combine_validities(values.validity(), regex.validity());

    let iterator = values.iter().zip(regex.iter()).map(|(haystack, regex)| {
        if haystack.is_none() | regex.is_none() {
            // regex is expensive => short-circuit if null
            return Result::Ok(false);
        };
        let haystack = haystack.unwrap();
        let regex = regex.unwrap();

        let regex = if let Some(regex) = map.get(regex) {
            regex
        } else {
            let re = Regex::new(regex).map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "Unable to build regex from LIKE pattern: {}",
                    e
                ))
            })?;
            map.insert(regex, re);
            map.get(regex).unwrap()
        };

        Ok(regex.is_match(haystack))
    });
    let new_values = unsafe { Bitmap::try_from_trusted_len_iter(iterator) }?;

    Ok(BooleanArray::from_data(new_values, validity))
}

/// Regex matches
/// # Example
/// ```
/// use arrow2::array::{Utf8Array, BooleanArray};
/// use arrow2::compute::regex_match::regex_match_scalar;
///
/// let strings = Utf8Array::<i32>::from_slice(&vec!["ArAow", "A_B", "AAA"]);
///
/// let result = regex_match_scalar(&strings, "^A.A").unwrap();
/// assert_eq!(result, BooleanArray::from_slice(&vec![true, false, true]));
/// ```
pub fn regex_match_scalar<O: Offset>(values: &Utf8Array<O>, regex: &str) -> Result<BooleanArray> {
    let regex = Regex::new(regex)
        .map_err(|e| ArrowError::InvalidArgumentError(format!("Unable to compile regex: {}", e)))?;
    Ok(unary_utf8_boolean(values, |x| regex.is_match(x)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_generic<O: Offset, F: Fn(&Utf8Array<O>, &Utf8Array<O>) -> Result<BooleanArray>>(
        lhs: Vec<&str>,
        pattern: Vec<&str>,
        op: F,
        expected: Vec<bool>,
    ) {
        let lhs = Utf8Array::<O>::from_slice(lhs);
        let pattern = Utf8Array::<O>::from_slice(pattern);
        let expected = BooleanArray::from_slice(expected);
        let result = op(&lhs, &pattern).unwrap();
        assert_eq!(result, expected);
    }

    fn test_generic_scalar<O: Offset, F: Fn(&Utf8Array<O>, &str) -> Result<BooleanArray>>(
        lhs: Vec<&str>,
        pattern: &str,
        op: F,
        expected: Vec<bool>,
    ) {
        let lhs = Utf8Array::<O>::from_slice(lhs);
        let expected = BooleanArray::from_slice(expected);
        let result = op(&lhs, &pattern).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_like() {
        test_generic::<i32, _>(
            vec![
                "arrow", "arrow", "arrow", "arrow", "arrow", "arrows", "arrow",
            ],
            vec!["arrow", "^ar", "ro", "foo", "arr$", "arrow.", "arrow."],
            regex_match,
            vec![true, true, true, false, false, true, false],
        )
    }

    #[test]
    fn test_like_scalar() {
        test_generic_scalar::<i32, _>(
            vec!["arrow", "parquet", "datafusion", "flight"],
            "ar",
            regex_match_scalar,
            vec![true, true, false, false],
        );

        test_generic_scalar::<i32, _>(
            vec!["arrow", "parquet", "datafusion", "flight"],
            "^ar",
            regex_match_scalar,
            vec![true, false, false, false],
        )
    }
}
