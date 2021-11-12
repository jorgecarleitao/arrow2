#![doc = include_str!("doc/lib.md")]
// So that we have more control over what is `unsafe` inside an `unsafe` block
#![allow(unused_unsafe)]
//
#![allow(clippy::len_without_is_empty)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[macro_use]
pub mod array;
#[cfg(feature = "cache_aligned")]
mod alloc;
pub mod bitmap;
pub mod buffer;
pub mod error;
pub mod scalar;
pub mod trusted_len;
pub mod types;
#[cfg(feature = "cache_aligned")]
mod vec;

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
