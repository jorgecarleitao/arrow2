//! Misc utilities used in different places in the crate.

#[cfg(any(feature = "compute", feature = "io_csv"))]
mod lexical;
#[cfg(any(feature = "compute", feature = "io_csv"))]
pub use lexical::*;

#[cfg(feature = "benchmarks")]
pub mod bench_util;
