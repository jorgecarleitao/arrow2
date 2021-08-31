//! APIs to read from and write to Arrow's IPC format.

#![allow(missing_debug_implementations)]
#![allow(non_camel_case_types)]
#[allow(clippy::redundant_closure)]
#[allow(clippy::needless_lifetimes)]
#[allow(clippy::extra_unused_lifetimes)]
#[allow(clippy::redundant_static_lifetimes)]
#[allow(clippy::redundant_field_names)]
pub mod gen;

mod compression;
mod convert;

pub use convert::fb_to_schema;
pub(crate) use convert::get_extension;
pub use gen::Message::root_as_message;
pub mod read;
pub mod write;

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
