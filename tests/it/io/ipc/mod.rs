mod common;
mod read;
mod write;

pub use common::read_gzip_json;

#[cfg(feature = "io_ipc_write_async")]
mod write_async;
