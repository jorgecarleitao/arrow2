//! `async` writing of arrow streams
use std::sync::Arc;

use futures::AsyncWrite;

use super::super::IpcField;
pub use super::common::WriteOptions;
use super::common::{encode_chunk, DictionaryTracker, EncodedData};
use super::common_async::{write_continuation, write_message};
use super::{default_ipc_fields, schema_to_bytes};

use crate::array::Array;
use crate::chunk::Chunk;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// An `async` writer to the Apache Arrow stream format.
pub struct StreamWriter<W: AsyncWrite + Unpin + Send> {
    /// The object to write to
    writer: W,
    /// IPC write options
    write_options: WriteOptions,
    /// Whether the stream has been finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,
}

impl<W: AsyncWrite + Unpin + Send> StreamWriter<W> {
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
    pub async fn start(&mut self, schema: &Schema, ipc_fields: Option<&[IpcField]>) -> Result<()> {
        let encoded_message = if let Some(ipc_fields) = ipc_fields {
            EncodedData {
                ipc_message: schema_to_bytes(schema, ipc_fields),
                arrow_data: vec![],
            }
        } else {
            let ipc_fields = default_ipc_fields(schema.fields());
            EncodedData {
                ipc_message: schema_to_bytes(schema, &ipc_fields),
                arrow_data: vec![],
            }
        };
        write_message(&mut self.writer, encoded_message).await?;
        Ok(())
    }

    /// Writes [`Chunk`] to the stream
    pub async fn write(
        &mut self,
        columns: &Chunk<Arc<dyn Array>>,
        schema: &Schema,
        ipc_fields: Option<&[IpcField]>,
    ) -> Result<()> {
        if self.finished {
            return Err(ArrowError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Cannot write to a finished stream".to_string(),
            )));
        }

        let (encoded_dictionaries, encoded_message) = if let Some(ipc_fields) = ipc_fields {
            encode_chunk(
                columns,
                ipc_fields,
                &mut self.dictionary_tracker,
                &self.write_options,
            )?
        } else {
            let ipc_fields = default_ipc_fields(schema.fields());
            encode_chunk(
                columns,
                &ipc_fields,
                &mut self.dictionary_tracker,
                &self.write_options,
            )?
        };

        for encoded_dictionary in encoded_dictionaries {
            write_message(&mut self.writer, encoded_dictionary).await?;
        }

        write_message(&mut self.writer, encoded_message).await?;
        Ok(())
    }

    /// Finishes the stream
    pub async fn finish(&mut self) -> Result<()> {
        write_continuation(&mut self.writer, 0).await?;
        self.finished = true;
        Ok(())
    }

    /// Consumes itself, returning the inner writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}
