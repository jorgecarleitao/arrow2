//! Boolean operators of [Kleene logic](https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics).
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::{
    array::BooleanArray,
    bitmap::{quaternary, ternary},
};

/// Logical 'or' with [Kleene logic](https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics)
/// # Errors
/// This function errors if the operands have different lengths.
/// # Example
///
/// ```rust
/// # use arrow2::error::Result;
/// use arrow2::array::BooleanArray;
/// use arrow2::compute::boolean_kleene::or;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(&[Some(true), Some(false), None]);
/// let b = BooleanArray::from(&[None, None, None]);
/// let or_ab = or(&a, &b)?;
/// assert_eq!(or_ab, BooleanArray::from(&[Some(true), None, None]));
/// # Ok(())
/// # }
/// ```
pub fn or(lhs: &BooleanArray, rhs: &BooleanArray) -> Result<BooleanArray> {
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let lhs_values = lhs.values();
    let rhs_values = rhs.values();

    let lhs_validity = lhs.validity();
    let rhs_validity = rhs.validity();

    let validity = match (lhs_validity, rhs_validity) {
        (Some(lhs_validity), Some(rhs_validity)) => {
            Some(quaternary(
                lhs_values,
                rhs_values,
                lhs_validity,
                rhs_validity,
                // see https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics
                |lhs, rhs, lhs_v, rhs_v| {
                    // A = T
                    (lhs & lhs_v) |
                    // B = T
                    (rhs & rhs_v) |
                    // A = F & B = F
                    (!lhs & lhs_v) & (!rhs & rhs_v)
                },
            ))
        }
        (Some(lhs_validity), None) => {
            // B != U
            Some(ternary(
                lhs_values,
                rhs_values,
                lhs_validity,
                // see https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics
                |lhs, rhs, lhs_v| {
                    // A = T
                    (lhs & lhs_v) |
                    // B = T
                    rhs |
                    // A = F & B = F
                    (!lhs & lhs_v) & !rhs
                },
            ))
        }
        (None, Some(rhs_validity)) => {
            Some(ternary(
                lhs_values,
                rhs_values,
                rhs_validity,
                // see https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics
                |lhs, rhs, rhs_v| {
                    // A = T
                    lhs |
                    // B = T
                    (rhs & rhs_v) |
                    // A = F & B = F
                    !lhs & (!rhs & rhs_v)
                },
            ))
        }
        (None, None) => None,
    };
    Ok(BooleanArray::from_data(
        DataType::Boolean,
        lhs_values | rhs_values,
        validity,
    ))
}

/// Logical 'and' with [Kleene logic](https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics)
/// # Errors
/// This function errors if the operands have different lengths.
/// # Example
///
/// ```rust
/// # use arrow2::error::Result;
/// use arrow2::array::BooleanArray;
/// use arrow2::compute::boolean_kleene::and;
/// # fn main() -> Result<()> {
/// let a = BooleanArray::from(&[Some(true), Some(false), None]);
/// let b = BooleanArray::from(&[None, None, None]);
/// let and_ab = and(&a, &b)?;
/// assert_eq!(and_ab, BooleanArray::from(&[None, Some(false), None]));
/// # Ok(())
/// # }
/// ```
pub fn and(lhs: &BooleanArray, rhs: &BooleanArray) -> Result<BooleanArray> {
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let lhs_values = lhs.values();
    let rhs_values = rhs.values();

    let lhs_validity = lhs.validity();
    let rhs_validity = rhs.validity();

    let validity = match (lhs_validity, rhs_validity) {
        (Some(lhs_validity), Some(rhs_validity)) => {
            Some(quaternary(
                lhs_values,
                rhs_values,
                lhs_validity,
                rhs_validity,
                // see https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics
                |lhs, rhs, lhs_v, rhs_v| {
                    // B = F
                    (!rhs & rhs_v) |
                    // A = F
                    (!lhs & lhs_v) |
                    // A = T & B = T
                    (lhs & lhs_v) & (rhs & rhs_v)
                },
            ))
        }
        (Some(lhs_validity), None) => {
            Some(ternary(
                lhs_values,
                rhs_values,
                lhs_validity,
                // see https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics
                |lhs, rhs, lhs_v| {
                    // B = F
                    !rhs |
                    // A = F
                    (!lhs & lhs_v) |
                    // A = T & B = T
                    (lhs & lhs_v) & rhs
                },
            ))
        }
        (None, Some(rhs_validity)) => {
            Some(ternary(
                lhs_values,
                rhs_values,
                rhs_validity,
                // see https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics
                |lhs, rhs, rhs_v| {
                    // B = F
                    (!rhs & rhs_v) |
                    // A = F
                    !lhs |
                    // A = T & B = T
                    lhs & (rhs & rhs_v)
                },
            ))
        }
        (None, None) => None,
    };
    Ok(BooleanArray::from_data(
        DataType::Boolean,
        lhs_values & rhs_values,
        validity,
    ))
}
