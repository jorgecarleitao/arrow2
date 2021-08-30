#![deny(missing_docs)]
//! Contains containers for all Arrow sized types (e.g. `i32`),
//! [`Buffer`] and [`MutableBuffer`].

mod immutable;
mod mutable;

pub(crate) mod bytes;
pub(crate) mod util;

pub use immutable::Buffer;
pub use mutable::MutableBuffer;
