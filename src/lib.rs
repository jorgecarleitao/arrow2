//! Doc provided by README

#![cfg_attr(docsrs, feature(doc_cfg))]

#[macro_use]
pub mod array;
pub mod bitmap;
pub mod buffer;
mod endianess;
pub mod error;
pub mod scalar;
pub mod trusted_len;
pub mod types;

#[cfg(feature = "compute")]
#[cfg_attr(docsrs, doc(cfg(feature = "compute")))]
pub mod compute;
pub mod io;
pub mod record_batch;
pub mod temporal_conversions;

pub mod datatypes;

pub mod ffi;
pub mod util;

// so that documentation gets test
#[cfg(any(test, doctest))]
mod docs;
