//! Defines generics suitable to perform operations to [`PrimitiveArray`] in-place.

use super::utils::check_same_len;
use crate::{array::PrimitiveArray, types::NativeType};

/// Applies an unary function to a [`PrimitiveArray`] in-place via cow semantics.
///
/// # Implementation
/// This is the fastest method to apply a binary operation and it is often vectorized (SIMD).
/// # Panics
/// This function panics iff
/// * the arrays have a different length.
/// * the function itself panics.
#[inline]
pub fn unary<I, F>(array: &mut PrimitiveArray<I>, op: F)
where
    I: NativeType,
    F: Fn(I) -> I,
{
    array.apply_values(|values| values.iter_mut().for_each(|v| *v = op(*v)));
}

/// Applies a binary operations to two [`PrimitiveArray`], applying the operation
/// in-place to the `lhs` via cow semantics.
///
/// # Implementation
/// This is the fastest way to perform a binary operation and it is often vectorized (SIMD).
/// # Panics
/// This function panics iff
/// * the arrays have a different length.
/// * the function itself panics.
#[inline]
pub fn binary<T, D, F>(lhs: &mut PrimitiveArray<T>, rhs: &PrimitiveArray<D>, op: F)
where
    T: NativeType,
    D: NativeType,
    F: Fn(T, D) -> T,
{
    check_same_len(lhs, rhs).unwrap();

    match rhs.validity() {
        None => {}
        Some(rhs) => {
            if lhs.validity().is_none() {
                *lhs = lhs.with_validity(Some(rhs.clone()))
            } else {
                lhs.apply_validity(|mut lhs| lhs &= rhs)
            }
        }
    }

    lhs.apply_values(|x| {
        x.iter_mut()
            .zip(rhs.values().iter())
            .for_each(|(l, r)| *l = op(*l, *r))
    });
}
