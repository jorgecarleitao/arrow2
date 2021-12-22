//! Async read Avro

mod block;
mod metadata;
pub(self) mod utils;

pub use super::{Block, CompressedBlock};
pub use block::block_stream;
pub use metadata::read_metadata;
