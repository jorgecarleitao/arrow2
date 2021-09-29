#![deny(missing_docs)]
//! Contains [`Buffer`] and [`MutableBuffer`], containers for all Arrow
//! physical types (e.g. i32, f64).

mod immutable;
mod mutable;

pub(crate) mod bytes;

pub use immutable::Buffer;
pub use mutable::MutableBuffer;
