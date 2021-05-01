mod alloc;
pub mod array;
pub mod bitmap;
pub mod buffer;
mod endianess;
pub mod error;
pub mod trusted_len;
pub mod types;

pub mod compute;
pub mod io;
pub mod record_batch;
pub mod temporal_conversions;
pub use alloc::total_allocated_bytes;

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
