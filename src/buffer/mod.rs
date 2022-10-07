//! Contains [`Buffer`], an immutable container for all Arrow physical types (e.g. i32, f64).

mod immutable;
mod iterator;

use crate::ffi::InternalArrowArray;

pub(crate) type Bytes<T> = foreign_vec::ForeignVec<InternalArrowArray, T>;
pub(super) use iterator::IntoIter;

pub use immutable::Buffer;
