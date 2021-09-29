#![deny(missing_docs)]
//! contains [`Bitmap`] and [`MutableBitmap`], containers of `bool`.
mod immutable;
pub use immutable::*;

mod mutable;
pub use mutable::MutableBitmap;

mod bitmap_ops;
pub use bitmap_ops::*;

pub mod utils;
