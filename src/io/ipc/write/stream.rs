//! Arrow IPC File and Stream Writers
//!
//! The `FileWriter` and `StreamWriter` have similar interfaces,
//! however the `FileWriter` expects a reader that supports `Seek`ing

use std::io::Write;
use std::sync::Arc;

use super::super::IpcField;
use super::common::{encode_columns, DictionaryTracker, EncodedData, WriteOptions};
use super::common_sync::{write_continuation, write_message};
use super::schema_to_bytes;

use crate::array::Array;
use crate::columns::Columns;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// Arrow stream writer
///
/// The data written by this writer must be read in order. To signal that no more
/// data is arriving through the stream call [`self.finish()`](StreamWriter::finish);
///
/// For a usage walkthrough consult [this example](https://github.com/jorgecarleitao/arrow2/tree/main/examples/ipc_pyarrow).
pub struct StreamWriter<W: Write> {
    /// The object to write to
    writer: W,
    /// IPC write options
    write_options: WriteOptions,
    /// Whether the stream has been finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,
}

impl<W: Write> StreamWriter<W> {
    /// Creates a new [`StreamWriter`]
    pub fn new(writer: W, write_options: WriteOptions) -> Self {
        Self {
            writer,
            write_options,
            finished: false,
            dictionary_tracker: DictionaryTracker::new(false),
        }
    }

    /// Starts the stream
    pub fn start(&mut self, schema: &Schema, ipc_fields: &[IpcField]) -> Result<()> {
        let encoded_message = EncodedData {
            ipc_message: schema_to_bytes(schema, ipc_fields),
            arrow_data: vec![],
        };
        write_message(&mut self.writer, encoded_message)?;
        Ok(())
    }

    /// Writes [`Columns`] to the stream
    pub fn write(&mut self, columns: &Columns<Arc<dyn Array>>, fields: &[IpcField]) -> Result<()> {
        if self.finished {
            return Err(ArrowError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Cannot write to a finished stream".to_string(),
            )));
        }

        let (encoded_dictionaries, encoded_message) = encode_columns(
            columns,
            fields,
            &mut self.dictionary_tracker,
            &self.write_options,
        )?;

        for encoded_dictionary in encoded_dictionaries {
            write_message(&mut self.writer, encoded_dictionary)?;
        }

        write_message(&mut self.writer, encoded_message)?;
        Ok(())
    }

    /// Write continuation bytes, and mark the stream as done
    pub fn finish(&mut self) -> Result<()> {
        write_continuation(&mut self.writer, 0)?;

        self.finished = true;

        Ok(())
    }

    /// Consumes itself, returning the inner writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}
