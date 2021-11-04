//! APIs to write to Arrow's IPC format.
pub mod common;
mod schema;
mod serialize;
mod stream;
mod writer;

pub use common::{Compression, WriteOptions};
pub use schema::schema_to_bytes;
pub use serialize::{write, write_dictionary};
pub use stream::StreamWriter;
pub use writer::FileWriter;
