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
//! allows only [`RecordBatch`](crate::record_batch::RecordBatch)es or
//! [`DictionaryBatch`](gen::Message::DictionaryBatch) to be passed
//! around due to its reliance on a pre-defined data scheme. This limitation
//! provides a large performance gain because serialized data will always have a
//! known structutre, i.e. the same fields and datatypes, with the only variance
//! being the number of rows and the actual data inside the Batch. This dramatically
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
//! # Examples
//! Read and write to a file:
//! ```
//! use arrow2::io::ipc::{{read::{FileReader, read_file_metadata}}, {write::FileWriter}};
//! # use std::fs::File;
//! # use std::sync::Arc;
//! # use arrow2::datatypes::{Field, Schema, DataType};
//! # use arrow2::array::Int32Array;
//! # use arrow2::record_batch::RecordBatch;
//! // Setup the writer
//! let path = "/tmp/example.dat".to_string();
//! let mut file = File::create(&path).unwrap();
//! let x_coord = Field::new("x", DataType::Int32, false);
//! let y_coord = Field::new("y", DataType::Int32, false);
//! let schema = Schema::new(vec![x_coord, y_coord]);
//! let mut writer = FileWriter::try_new(file, &schema).unwrap();
//!
//! // Setup the data
//! let x_data = Int32Array::from_slice([-1i32, 1]);
//! let y_data = Int32Array::from_slice([1i32, -1]);
//! let batch = RecordBatch::try_new(
//!        Arc::new(schema),
//!         vec![Arc::new(x_data), Arc::new(y_data)]
//!    ).unwrap();
//!
//! // Write the messages and finalize the stream
//! for _ in 0..5 {
//!     writer.write(&batch);
//! }
//! writer.finish();
//!
//! // Fetch some of the data and get the reader back
//! let mut reader = File::open(&path).unwrap();
//! let metadata = read_file_metadata(&mut reader).unwrap();
//! let mut filereader = FileReader::new(reader, metadata, None);
//! let row1 = filereader.next().unwrap();  // [[-1, 1], [1, -1]]
//! let row2 = filereader.next().unwrap();  // [[-1, 1], [1, -1]]
//! let mut reader = filereader.into_inner();
//! // Do more stuff with the reader, like seeking ahead.
//!
//! ```
//!
//! For further information and examples please consult the
//! [user guide](https://jorgecarleitao.github.io/arrow2/io/index.html).
//! For even more examples check the `examples` folder in the main repository
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
