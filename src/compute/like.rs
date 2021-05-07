use std::collections::HashMap;

use regex::Regex;

use crate::{array::*, bitmap::Bitmap};
use crate::{
    compute::utils::combine_validities,
    error::{ArrowError, Result},
};

#[inline]
fn is_like_pattern(c: char) -> bool {
    c == '%' || c == '_'
}

#[inline]
fn a_like_utf8<O: Offset, F: Fn(bool) -> bool>(
    lhs: &Utf8Array<O>,
    rhs: &Utf8Array<O>,
    op: F,
) -> Result<BooleanArray> {
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot perform comparison operation on arrays of different length".to_string(),
        ));
    }

    let validity = combine_validities(lhs.validity(), rhs.validity());

    let mut map = HashMap::new();

    let values =
        Bitmap::try_from_trusted_len_iter(lhs.iter().zip(rhs.iter()).map(|(lhs, rhs)| {
            match (lhs, rhs) {
                (Some(lhs), Some(pattern)) => {
                    let pattern = if let Some(pattern) = map.get(pattern) {
                        pattern
                    } else {
                        let re_pattern = pattern.replace("%", ".*").replace("_", ".");
                        let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
                            ArrowError::InvalidArgumentError(format!(
                                "Unable to build regex from LIKE pattern: {}",
                                e
                            ))
                        })?;
                        map.insert(pattern, re);
                        map.get(pattern).unwrap()
                    };
                    Result::Ok(op(pattern.is_match(lhs)))
                }
                _ => Ok(false),
            }
        }))?;

    Ok(BooleanArray::from_data(values, validity))
}

/// Returns `lhs LIKE rhs` operation on two [`Utf8Array`].
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
///
/// # Error
/// Errors iff:
/// * the arrays have a different length
/// * any of the patterns is not valid
/// # Example
/// ```
/// use arrow2::array::{Utf8Array, BooleanArray};
/// use arrow2::compute::like::like_utf8;
///
/// let strings = Utf8Array::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "Arrow", "Ar"]);
/// let patterns = Utf8Array::<i32>::from_slice(&["A%", "B%", "%r_ow", "A_", "A_"]);
///
/// let result = like_utf8(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from_slice(&[true, false, true, false, true]));
/// ```
pub fn like_utf8<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    a_like_utf8(lhs, rhs, |x| x)
}

pub fn nlike_utf8<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    a_like_utf8(lhs, rhs, |x| !x)
}

fn a_like_utf8_scalar<O: Offset, F: Fn(bool) -> bool>(
    lhs: &Utf8Array<O>,
    rhs: &str,
    op: F,
) -> Result<BooleanArray> {
    let validity = lhs.validity();

    let values = if !rhs.contains(is_like_pattern) {
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| x == rhs))
    } else if rhs.ends_with('%') && !rhs[..rhs.len() - 1].contains(is_like_pattern) {
        // fast path, can use starts_with
        let starts_with = &rhs[..rhs.len() - 1];
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x.starts_with(starts_with))))
    } else if rhs.starts_with('%') && !rhs[1..].contains(is_like_pattern) {
        // fast path, can use ends_with
        let ends_with = &rhs[1..];
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x.ends_with(ends_with))))
    } else {
        let re_pattern = rhs.replace("%", ".*").replace("_", ".");
        let re = Regex::new(&format!("^{}$", re_pattern)).map_err(|e| {
            ArrowError::InvalidArgumentError(format!(
                "Unable to build regex from LIKE pattern: {}",
                e
            ))
        })?;
        Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(re.is_match(x))))
    };
    Ok(BooleanArray::from_data(values, validity.clone()))
}

/// Returns `lhs LIKE rhs` operation.
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
///
/// # Error
/// Errors iff:
/// * the arrays have a different length
/// * any of the patterns is not valid
/// # Example
/// ```
/// use arrow2::array::{Utf8Array, BooleanArray};
/// use arrow2::compute::like::like_utf8_scalar;
///
/// let array = Utf8Array::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "BA"]);
///
/// let result = like_utf8_scalar(&array, &"A%").unwrap();
/// assert_eq!(result, BooleanArray::from_slice(&[true, true, true, false]));
/// ```
pub fn like_utf8_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> Result<BooleanArray> {
    a_like_utf8_scalar(lhs, rhs, |x| x)
}

pub fn nlike_utf8_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> Result<BooleanArray> {
    a_like_utf8_scalar(lhs, rhs, |x| !x)
}
