//! Contains arithemtic functions for [`PrimitiveArray`](crate::array::PrimitiveArray)s.
//!
//! Each operation has four variants, like the rest of Rust's ecosystem:
//! * usual, that [`panic!`]s on overflow
//! * `checked_*` that turns overflowings to `None`
//! * `overflowing_*` returning a [`Bitmap`](crate::bitmap::Bitmap) with items that overflow.
//! * `saturating_*` that saturates the result.
mod add;
pub use add::*;
mod div;
pub use div::*;
mod mul;
pub use mul::*;
mod pow;
pub use pow::*;
mod rem;
pub use rem::*;
mod sub;
pub use sub::*;
