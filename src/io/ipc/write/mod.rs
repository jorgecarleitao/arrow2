//! APIs to write to Arrow's IPC format.
pub(crate) mod common;
mod schema;
mod serialize;
mod stream;
mod writer;

pub use common::{Compression, WriteOptions};
pub use schema::schema_to_bytes;
pub use serialize::{write, write_dictionary};
pub use stream::StreamWriter;
pub use writer::FileWriter;

pub(crate) mod common_sync;

#[cfg(feature = "io_ipc_write_async")]
mod common_async;
#[cfg(feature = "io_ipc_write_async")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_ipc_write_async")))]
pub mod stream_async;
