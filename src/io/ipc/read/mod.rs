//! APIs to read Arrow's IPC format.
mod array;
mod common;
mod deserialize;
mod read_basic;
mod reader;
mod stream;

pub use common::{read_dictionary, read_record_batch};
pub use reader::{read_file_metadata, FileMetadata, FileReader};
pub use stream::{read_stream_metadata, StreamMetadata, StreamReader, StreamState};
