//! Contains efficient containers of booleans: [`Bitmap`] and [`MutableBitmap`].
//! The memory backing these containers is cache-aligned and optimized for both vertical
//! and horizontal operations over booleans.
mod immutable;
pub use immutable::*;

mod mutable;
pub use mutable::MutableBitmap;

mod iterator;
pub use iterator::BitmapIter;

mod bitmap_ops;
pub use bitmap_ops::*;
