//! Misc utilities used in different places in the crate.

#[cfg(any(feature = "compute", feature = "read_csv", feature = "write_csv"))]
mod lexical;
#[cfg(any(feature = "compute", feature = "read_csv", feature = "write_csv"))]
pub use lexical::*;

#[cfg(feature = "benchmarks")]
#[cfg_attr(docsrs, doc(cfg(feature = "benchmarks")))]
pub mod bench_util;
