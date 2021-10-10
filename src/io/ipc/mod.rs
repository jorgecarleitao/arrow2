//! APIs to read from and write to Arrow's IPC format.

mod compression;
mod convert;
mod endianess;

pub use convert::fb_to_schema;
pub mod read;
pub mod write;

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
