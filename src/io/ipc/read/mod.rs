//! APIs to read Arrow's IPC format.
//!
//! The two important structs here are the [`FileReader`](reader::FileReader),
//! which provides arbitrary access to any of its messages, and the
//! [`StreamReader`](stream::StreamReader), which only supports reading
//! data in the order it was written in.
use std::collections::HashMap;

use crate::array::Array;

mod array;
mod common;
mod deserialize;
mod error;
mod read_basic;
pub(crate) mod reader;
mod schema;
mod stream;

pub use error::OutOfSpecKind;

#[cfg(feature = "io_ipc_read_async")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_ipc_read_async")))]
pub mod stream_async;

#[cfg(feature = "io_ipc_read_async")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_ipc_read_async")))]
pub mod file_async;

pub use common::{read_dictionary, read_record_batch};
pub use reader::{
    read_batch, read_file_dictionaries, read_file_metadata, FileMetadata, FileReader,
};
pub use schema::deserialize_schema;
pub use stream::{read_stream_metadata, StreamMetadata, StreamReader, StreamState};

/// how dictionaries are tracked in this crate
pub type Dictionaries = HashMap<i64, Box<dyn Array>>;

pub(crate) type Node<'a> = arrow_format::ipc::FieldNodeRef<'a>;
pub(crate) type IpcBuffer<'a> = arrow_format::ipc::BufferRef<'a>;
pub(crate) type Compression<'a> = arrow_format::ipc::BodyCompressionRef<'a>;
pub(crate) type Version = arrow_format::ipc::MetadataVersion;
