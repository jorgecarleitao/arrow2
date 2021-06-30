//! This module contains core functionality to handle memory in this crate.
//!
//! The core containers of this module are [`MutableBuffer`] and [`Buffer`].
//! [`MutableBuffer`] is like [`Vec`], with the following main differences:
//! * it only supports types that implement [`super::types::NativeType`]
//! * it allocates memory along cache lines.
//! * it is not clonable.
//! [`Buffer`] is the immutable counterpart of [`MutableBuffer`].

mod immutable;
mod mutable;

pub(crate) mod bytes;
pub(crate) mod util;

pub use immutable::Buffer;
pub use mutable::MutableBuffer;
