//! APIs to read Arrow's IPC format.
//!
//! The two important structs here are the [`FileReader`](reader::FileReader),
//! which provides arbitrary access to any of its messages, and the
//! [`StreamReader`](stream::StreamReader), which only supports reading
//! data in the order it was written in.

mod array;
mod common;
mod deserialize;
mod read_basic;
mod reader;
mod stream;

pub use common::{read_dictionary, read_record_batch};
pub use reader::{read_file_metadata, FileMetadata, FileReader};
pub use stream::{read_stream_metadata, StreamMetadata, StreamReader, StreamState};
