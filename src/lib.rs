mod alloc;
pub mod array;
pub mod bitmap;
pub mod buffer;
pub mod error;
pub mod types;

pub mod compute;
pub mod io;
pub mod record_batch;
pub mod temporal_conversions;

#[cfg(feature = "benchmarks")]
pub mod bits;
#[cfg(not(feature = "benchmarks"))]
pub(crate) mod bits;
pub mod datatypes;

pub mod ffi;
pub mod util;

// so that documentation gets test
#[cfg(any(test, doctest))]
mod docs;
