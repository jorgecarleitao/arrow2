//! Contains "like" operators such as [`like_utf8`] and [`like_utf8_scalar`].
use std::collections::HashMap;

use regex::bytes::Regex as BytesRegex;
use regex::Regex;

use crate::datatypes::DataType;
use crate::{array::*, bitmap::Bitmap};
use crate::{
    compute::utils::combine_validities,
    error::{ArrowError, Result},
};

#[inline]
fn is_like_pattern_escape(c: char) -> bool {
    c == '%' || c == '_' || c == '\\'
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
                        let re_pattern = like_pattern_to_regex(pattern);
                        let re = Regex::new(&re_pattern).map_err(|e| {
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

    Ok(BooleanArray::from_data(DataType::Boolean, values, validity))
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

/// Returns `lhs NOT LIKE rhs` operation on two [`Utf8Array`].
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
pub fn nlike_utf8<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>) -> Result<BooleanArray> {
    a_like_utf8(lhs, rhs, |x| !x)
}

fn a_like_utf8_scalar<O: Offset, F: Fn(bool) -> bool>(
    lhs: &Utf8Array<O>,
    rhs: &str,
    op: F,
) -> Result<BooleanArray> {
    let validity = lhs.validity();

    let values = match check_pattern_type(rhs) {
        PatternType::OrdinalStr => {
            Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| x == rhs))
        }
        PatternType::EndOfPercent => {
            // fast path, can use starts_with
            let starts_with = &rhs[..rhs.len() - 1];
            Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x.starts_with(starts_with))))
        }
        PatternType::StartOfPercent => {
            // fast path, can use ends_with
            let ends_with = &rhs[1..];
            Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x.ends_with(ends_with))))
        }
        PatternType::PatternStr => {
            let re_pattern = like_pattern_to_regex(rhs);
            let re = Regex::new(&re_pattern).map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "Unable to build regex from LIKE pattern: {}",
                    e
                ))
            })?;
            Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(re.is_match(x))))
        }
    };
    Ok(BooleanArray::from_data(
        DataType::Boolean,
        values,
        validity.cloned(),
    ))
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

/// Returns `lhs NOT LIKE rhs` operation.
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
pub fn nlike_utf8_scalar<O: Offset>(lhs: &Utf8Array<O>, rhs: &str) -> Result<BooleanArray> {
    a_like_utf8_scalar(lhs, rhs, |x| !x)
}

#[inline]
fn a_like_binary<O: Offset, F: Fn(bool) -> bool>(
    lhs: &BinaryArray<O>,
    rhs: &BinaryArray<O>,
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
                        let pattern_str = simdutf8::basic::from_utf8(pattern).map_err(|e| {
                            ArrowError::InvalidArgumentError(format!(
                                "Unable to convert the LIKE pattern to string: {}",
                                e
                            ))
                        })?;
                        let re_pattern = like_pattern_to_regex(pattern_str);
                        let re = BytesRegex::new(&re_pattern).map_err(|e| {
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

    Ok(BooleanArray::from_data(DataType::Boolean, values, validity))
}

/// Returns `lhs LIKE rhs` operation on two [`BinaryArray`].
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
/// use arrow2::array::{BinaryArray, BooleanArray};
/// use arrow2::compute::like::like_binary;
///
/// let strings = BinaryArray::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "Arrow", "Ar"]);
/// let patterns = BinaryArray::<i32>::from_slice(&["A%", "B%", "%r_ow", "A_", "A_"]);
///
/// let result = like_binary(&strings, &patterns).unwrap();
/// assert_eq!(result, BooleanArray::from_slice(&[true, false, true, false, true]));
/// ```
pub fn like_binary<O: Offset>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>) -> Result<BooleanArray> {
    a_like_binary(lhs, rhs, |x| x)
}

/// Returns `lhs NOT LIKE rhs` operation on two [`BinaryArray`]s.
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
///
pub fn nlike_binary<O: Offset>(lhs: &BinaryArray<O>, rhs: &BinaryArray<O>) -> Result<BooleanArray> {
    a_like_binary(lhs, rhs, |x| !x)
}

fn a_like_binary_scalar<O: Offset, F: Fn(bool) -> bool>(
    lhs: &BinaryArray<O>,
    rhs: &[u8],
    op: F,
) -> Result<BooleanArray> {
    let validity = lhs.validity();
    let pattern = simdutf8::basic::from_utf8(rhs).map_err(|e| {
        ArrowError::InvalidArgumentError(format!(
            "Unable to convert the LIKE pattern to string: {}",
            e
        ))
    })?;

    let values = match check_pattern_type(pattern) {
        PatternType::OrdinalStr => {
            Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| x == rhs))
        }
        PatternType::EndOfPercent => {
            // fast path, can use starts_with
            let starts_with = &rhs[..rhs.len() - 1];
            Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x.starts_with(starts_with))))
        }
        PatternType::StartOfPercent => {
            // fast path, can use ends_with
            let ends_with = &rhs[1..];
            Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(x.ends_with(ends_with))))
        }
        PatternType::PatternStr => {
            let re_pattern = like_pattern_to_regex(pattern);
            let re = BytesRegex::new(&re_pattern).map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "Unable to build regex from LIKE pattern: {}",
                    e
                ))
            })?;
            Bitmap::from_trusted_len_iter(lhs.values_iter().map(|x| op(re.is_match(x))))
        }
    };
    Ok(BooleanArray::from_data(
        DataType::Boolean,
        values,
        validity.cloned(),
    ))
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
/// use arrow2::array::{BinaryArray, BooleanArray};
/// use arrow2::compute::like::like_binary_scalar;
///
/// let array = BinaryArray::<i32>::from_slice(&["Arrow", "Arrow", "Arrow", "BA"]);
///
/// let result = like_binary_scalar(&array, b"A%").unwrap();
/// assert_eq!(result, BooleanArray::from_slice(&[true, true, true, false]));
/// ```
pub fn like_binary_scalar<O: Offset>(lhs: &BinaryArray<O>, rhs: &[u8]) -> Result<BooleanArray> {
    a_like_binary_scalar(lhs, rhs, |x| x)
}

/// Returns `lhs NOT LIKE rhs` operation on two [`BinaryArray`]s.
///
/// There are two wildcards supported:
///
/// * `%` - The percent sign represents zero, one, or multiple characters
/// * `_` - The underscore represents a single character
///
pub fn nlike_binary_scalar<O: Offset>(lhs: &BinaryArray<O>, rhs: &[u8]) -> Result<BooleanArray> {
    a_like_binary_scalar(lhs, rhs, |x| !x)
}

enum PatternType {
    // e.g. 'Arrow'
    OrdinalStr,
    // e.g. 'A%row'
    PatternStr,
    // e.g. '%rrow'
    StartOfPercent,
    // e.g. 'Arro%'
    EndOfPercent,
}

fn check_pattern_type(pattern: &str) -> PatternType {
    let start_percent = pattern.starts_with('%');
    let end_percent = pattern.ends_with('%') && !pattern.ends_with("\\%");

    let mut chars = if start_percent {
        pattern[1..].chars()
    } else if end_percent {
        pattern[..pattern.len() - 1].chars()
    } else {
        pattern.chars()
    };

    while let Some(c) = chars.next() {
        match c {
            '_' | '%' => return PatternType::PatternStr,
            '\\' => {
                if let Some(v) = chars.next() {
                    if is_like_pattern_escape(v) {
                        return PatternType::PatternStr;
                    }
                }
            }
            _ => {}
        }
    }

    if start_percent {
        PatternType::StartOfPercent
    } else if end_percent {
        PatternType::EndOfPercent
    } else {
        PatternType::OrdinalStr
    }
}

/// Transform the like pattern to regex pattern.
/// e.g. 'Hello\._World%\%' tranform to '^Hello\\\..World.*%$'.
pub(crate) fn like_pattern_to_regex(pattern: &str) -> String {
    let mut regex = String::with_capacity(pattern.len() * 2);
    regex.push('^');

    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            // Use double backslash to escape special character.
            '^' | '$' | '(' | ')' | '*' | '+' | '.' | '[' | '?' | '{' | '|' => {
                regex.push('\\');
                regex.push(c);
            }
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => {
                if let Some(v) = chars.peek().cloned() {
                    match v {
                        '%' | '_' => {
                            regex.push(v);
                            chars.next();
                        }
                        '\\' => {
                            regex.push_str("\\\\");
                            chars.next();
                        }
                        _ => regex.push_str("\\\\"),
                    };
                } else {
                    regex.push_str("\\\\");
                }
            }
            _ => regex.push(c),
        }
    }

    regex.push('$');
    regex
}
