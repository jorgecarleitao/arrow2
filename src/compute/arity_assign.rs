//! Defines generics suitable to perform operations to [`PrimitiveArray`] in-place.

use super::utils::check_same_len;
use crate::{array::PrimitiveArray, types::NativeType};
use either::Either;

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
    array.apply_values_mut(|values| values.iter_mut().for_each(|v| *v = op(*v)));
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

    // both for the validity and for the values
    // we branch to check if we can mutate in place
    // if we can, great that is fastest.
    // if we cannot, we allocate a new buffer and assign values to that
    // new buffer, that is benchmarked to be ~2x faster than first memcpy and assign in place
    // for the validity bits it can be much faster as we might need to iterate all bits if the
    // bitmap has an offset.
    if let Some(rhs) = rhs.validity() {
        if lhs.validity().is_none() {
            *lhs = lhs.with_validity(Some(rhs.clone()))
        } else {
            lhs.apply_validity(|bitmap| {
                // we need to take ownership for the `into_mut` call, but leave the `&mut` lhs intact
                // so that we can later assign the result to out `&mut bitmap`
                let owned_lhs = std::mem::take(bitmap);

                *bitmap = match owned_lhs.into_mut() {
                    // we take alloc and write to new buffer
                    Either::Left(immutable) => {
                        // we allocate a new bitmap
                        &immutable & rhs
                    }
                    // we can mutate in place, happy days.
                    Either::Right(mut mutable) => {
                        let mut mutable_ref = &mut mutable;
                        mutable_ref &= rhs;
                        mutable.into()
                    }
                }
            });
        }
    }

    // we need to take ownership for the `into_mut` call, but leave the `&mut` lhs intact
    // so that we can later assign the result to out `&mut lhs`
    let owned_lhs = std::mem::take(lhs);

    *lhs = match owned_lhs.into_mut() {
        // we take alloc and write to new buffer
        Either::Left(mut immutable) => {
            let values = immutable
                .values()
                .iter()
                .zip(rhs.values().iter())
                .map(|(l, r)| op(*l, *r))
                .collect::<Vec<_>>();
            immutable.set_values(values.into());
            immutable
        }
        // we can mutate in place
        Either::Right(mut mutable) => {
            mutable.apply_values(|x| {
                x.iter_mut()
                    .zip(rhs.values().iter())
                    .for_each(|(l, r)| *l = op(*l, *r))
            });
            mutable.into()
        }
    }
}
