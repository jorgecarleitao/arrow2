use std::io::Read;

use arrow_format;
use arrow_format::ipc::planus::ReadAsRoot;

use crate::array::Array;
use crate::chunk::Chunk;
use crate::datatypes::Schema;
use crate::error::{Error, Result};
use crate::io::ipc::IpcSchema;

use super::super::CONTINUATION_MARKER;
use super::common::*;
use super::schema::deserialize_stream_metadata;
use super::Dictionaries;

/// Metadata of an Arrow IPC stream, written at the start of the stream
#[derive(Debug, Clone)]
pub struct StreamMetadata {
    /// The schema that is read from the stream's first message
    pub schema: Schema,

    /// The IPC version of the stream
    pub version: arrow_format::ipc::MetadataVersion,

    /// The IPC fields tracking dictionaries
    pub ipc_schema: IpcSchema,
}

/// Reads the metadata of the stream
pub fn read_stream_metadata<R: Read>(reader: &mut R) -> Result<StreamMetadata> {
    // determine metadata length
    let mut meta_size: [u8; 4] = [0; 4];
    reader.read_exact(&mut meta_size)?;
    let meta_len = {
        // If a continuation marker is encountered, skip over it and read
        // the size from the next four bytes.
        if meta_size == CONTINUATION_MARKER {
            reader.read_exact(&mut meta_size)?;
        }
        i32::from_le_bytes(meta_size)
    };

    let mut meta_buffer = vec![0; meta_len as usize];
    reader.read_exact(&mut meta_buffer)?;

    deserialize_stream_metadata(&meta_buffer)
}

/// Encodes the stream's status after each read.
///
/// A stream is an iterator, and an iterator returns `Option<Item>`. The `Item`
/// type in the [`StreamReader`] case is `StreamState`, which means that an Arrow
/// stream may yield one of three values: (1) `None`, which signals that the stream
/// is done; (2) [`StreamState::Some`], which signals that there was
/// data waiting in the stream and we read it; and finally (3)
/// [`Some(StreamState::Waiting)`], which means that the stream is still "live", it
/// just doesn't hold any data right now.
pub enum StreamState {
    /// A live stream without data
    Waiting,
    /// Next item in the stream
    Some(Chunk<Box<dyn Array>>),
}

impl StreamState {
    /// Return the data inside this wrapper.
    ///
    /// # Panics
    ///
    /// If the `StreamState` was `Waiting`.
    pub fn unwrap(self) -> Chunk<Box<dyn Array>> {
        if let StreamState::Some(batch) = self {
            batch
        } else {
            panic!("The batch is not available")
        }
    }
}

/// Reads the next item, yielding `None` if the stream is done,
/// and a [`StreamState`] otherwise.
fn read_next<R: Read>(
    reader: &mut R,
    metadata: &StreamMetadata,
    dictionaries: &mut Dictionaries,
    message_buffer: &mut Vec<u8>,
    data_buffer: &mut Vec<u8>,
) -> Result<Option<StreamState>> {
    // determine metadata length
    let mut meta_length: [u8; 4] = [0; 4];

    match reader.read_exact(&mut meta_length) {
        Ok(()) => (),
        Err(e) => {
            return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                // Handle EOF without the "0xFFFFFFFF 0x00000000"
                // valid according to:
                // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                Ok(Some(StreamState::Waiting))
            } else {
                Err(Error::from(e))
            };
        }
    }

    let meta_length = {
        // If a continuation marker is encountered, skip over it and read
        // the size from the next four bytes.
        if meta_length == CONTINUATION_MARKER {
            reader.read_exact(&mut meta_length)?;
        }
        i32::from_le_bytes(meta_length) as usize
    };

    if meta_length == 0 {
        // the stream has ended, mark the reader as finished
        return Ok(None);
    }

    message_buffer.clear();
    message_buffer.resize(meta_length, 0);
    reader.read_exact(message_buffer)?;

    let message = arrow_format::ipc::MessageRef::read_as_root(message_buffer)
        .map_err(|err| Error::OutOfSpec(format!("Unable to get root as message: {:?}", err)))?;
    let header = message.header()?.ok_or_else(|| {
        Error::oos("IPC: unable to fetch the message header. The file or stream is corrupted.")
    })?;

    match header {
        arrow_format::ipc::MessageHeaderRef::Schema(_) => Err(Error::oos("A stream ")),
        arrow_format::ipc::MessageHeaderRef::RecordBatch(batch) => {
            // read the block that makes up the record batch into a buffer
            let length: usize = message
                .body_length()?
                .try_into()
                .map_err(|_| Error::oos("The body length of a header must be larger than zero"))?;
            data_buffer.clear();
            data_buffer.resize(length, 0);
            reader.read_exact(data_buffer)?;

            let file_size = data_buffer.len() as u64;

            let mut reader = std::io::Cursor::new(data_buffer);

            read_record_batch(
                batch,
                &metadata.schema.fields,
                &metadata.ipc_schema,
                None,
                dictionaries,
                metadata.version,
                &mut reader,
                0,
                file_size,
            )
            .map(|x| Some(StreamState::Some(x)))
        }
        arrow_format::ipc::MessageHeaderRef::DictionaryBatch(batch) => {
            // read the block that makes up the dictionary batch into a buffer
            let length: usize = message
                .body_length()?
                .try_into()
                .map_err(|_| Error::oos("The body length of a header must be larger than zero"))?;
            let mut buf = vec![0; length];
            reader.read_exact(&mut buf)?;

            let mut dict_reader = std::io::Cursor::new(&buf);

            read_dictionary(
                batch,
                &metadata.schema.fields,
                &metadata.ipc_schema,
                dictionaries,
                &mut dict_reader,
                0,
                buf.len() as u64,
            )?;

            // read the next message until we encounter a RecordBatch message
            read_next(reader, metadata, dictionaries, message_buffer, data_buffer)
        }
        t => Err(Error::OutOfSpec(format!(
            "Reading types other than record batches not yet supported, unable to read {:?} ",
            t
        ))),
    }
}

/// Arrow Stream reader.
///
/// An [`Iterator`] over an Arrow stream that yields a result of [`StreamState`]s.
/// This is the recommended way to read an arrow stream (by iterating over its data).
///
/// For a more thorough walkthrough consult [this example](https://github.com/jorgecarleitao/arrow2/tree/main/examples/ipc_pyarrow).
pub struct StreamReader<R: Read> {
    reader: R,
    metadata: StreamMetadata,
    dictionaries: Dictionaries,
    finished: bool,
    data_buffer: Vec<u8>,
    message_buffer: Vec<u8>,
}

impl<R: Read> StreamReader<R> {
    /// Try to create a new stream reader
    ///
    /// The first message in the stream is the schema, the reader will fail if it does not
    /// encounter a schema.
    /// To check if the reader is done, use `is_finished(self)`
    pub fn new(reader: R, metadata: StreamMetadata) -> Self {
        Self {
            reader,
            metadata,
            dictionaries: Default::default(),
            finished: false,
            data_buffer: vec![],
            message_buffer: vec![],
        }
    }

    /// Return the schema of the stream
    pub fn metadata(&self) -> &StreamMetadata {
        &self.metadata
    }

    /// Check if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    fn maybe_next(&mut self) -> Result<Option<StreamState>> {
        if self.finished {
            return Ok(None);
        }
        let batch = read_next(
            &mut self.reader,
            &self.metadata,
            &mut self.dictionaries,
            &mut self.message_buffer,
            &mut self.data_buffer,
        )?;
        if batch.is_none() {
            self.finished = true;
        }
        Ok(batch)
    }
}

impl<R: Read> Iterator for StreamReader<R> {
    type Item = Result<StreamState>;

    fn next(&mut self) -> Option<Self::Item> {
        self.maybe_next().transpose()
    }
}
