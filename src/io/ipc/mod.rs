//! APIs to read from and write to Arrow's IPC format.
//!
//! Inter-process communication is a method through which different processes
//! share and pass data between them. Its use-cases include parallel
//! processing of chunks of data across different CPU cores, transferring
//! data between different Apache Arrow implementations in other languages and
//! more. Under the hood Apache Arrow uses [FlatBuffers](https://google.github.io/flatbuffers/)
//! as its binary protocol, so every Arrow-centered streaming or serialiation
//! problem that could be solved using FlatBuffers could probably be solved
//! using the more integrated approach that is exposed in this module.
//!
//! [Arrow's IPC protocol](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
//! allows only [`RecordBatch`](crate::record_batch::RecordBatch)es to be passed
//! around due to its reliance on a pre-defined data scheme. This limitation
//! provides a large performance gain because serialized data will always have a
//! known structutre, i.e. the same fields and datatypes, with the only variance
//! being the number of rows and the actual data inside the RecordBatch. This dramatically
//! increases the deserialization rate, as the bytes in the file or stream are already
//! structured "correctly".
//!
//! Reading and writing IPC messages is done using one of two variants - either
//! [`FileReader`](read::FileReader) <-> [`FileWriter`](struct@write::FileWriter) or
//! [`StreamReader`](read::StreamReader) <-> [`StreamWriter`](struct@write::StreamWriter).
//! These two variants wrap a type `T` that implements [`Read`](std::io::Read), and in
//! the case of the `File` variant it also implements [`Seek`](std::io::Seek). In
//! practice it means that `File`s can be arbitrarily accessed while `Stream`s are only
//! read in certain order - the one they were written in (first in, first out).
//!
//! For further information and examples please consult the
//! [user guide](https://jorgecarleitao.github.io/arrow2/io/index.html).
//! For more examples check the [read](mod@read) and [write](mod@write) modules
//! or look at the `examples` folder in the main repository
//! ([1](https://github.com/jorgecarleitao/arrow2/blob/main/examples/ipc_file_read.rs),
//! [2](https://github.com/jorgecarleitao/arrow2/blob/main/examples/ipc_file_write.rs),
//! [3](https://github.com/jorgecarleitao/arrow2/tree/main/examples/ipc_pyarrow)).

#![allow(missing_debug_implementations)]
#![allow(non_camel_case_types)]

pub use convert::fb_to_schema;
pub use gen::Message::root_as_message;

#[allow(clippy::redundant_closure)]
#[allow(clippy::needless_lifetimes)]
#[allow(clippy::extra_unused_lifetimes)]
#[allow(clippy::redundant_static_lifetimes)]
#[allow(clippy::redundant_field_names)]
pub mod gen;

mod compression;
mod convert;

mod endianess;
pub mod read;
pub mod write;

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
