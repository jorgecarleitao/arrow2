use std::io::Write;

use arrow_format::ipc;
use arrow_format::ipc::flatbuffers::FlatBufferBuilder;

use super::{
    super::IpcField,
    super::ARROW_MAGIC,
    common::{encode_columns, DictionaryTracker, EncodedData, WriteOptions},
    common_sync::{write_continuation, write_message},
    default_ipc_fields, schema, schema_to_bytes,
};

use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

/// Arrow file writer
pub struct FileWriter<W: Write> {
    /// The object to write to
    writer: W,
    /// IPC write options
    options: WriteOptions,
    /// A reference to the schema, used in validating record batches
    schema: Schema,
    ipc_fields: Vec<IpcField>,
    /// The number of bytes between each block of bytes, as an offset for random access
    block_offsets: usize,
    /// Dictionary blocks that will be written as part of the IPC footer
    dictionary_blocks: Vec<ipc::File::Block>,
    /// Record blocks that will be written as part of the IPC footer
    record_blocks: Vec<ipc::File::Block>,
    /// Whether the writer footer has been written, and the writer is finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,
}

impl<W: Write> FileWriter<W> {
    /// Try create a new writer, with the schema written as part of the header
    pub fn try_new(
        mut writer: W,
        schema: &Schema,
        ipc_fields: Option<Vec<IpcField>>,
        options: WriteOptions,
    ) -> Result<Self> {
        // write magic to header
        writer.write_all(&ARROW_MAGIC[..])?;
        // create an 8-byte boundary after the header
        writer.write_all(&[0, 0])?;
        // write the schema, set the written bytes to the schema

        let ipc_fields = if let Some(ipc_fields) = ipc_fields {
            ipc_fields
        } else {
            default_ipc_fields(schema.fields())
        };
        let encoded_message = EncodedData {
            ipc_message: schema_to_bytes(schema, &ipc_fields),
            arrow_data: vec![],
        };

        let (meta, data) = write_message(&mut writer, encoded_message)?;
        Ok(Self {
            writer,
            options,
            schema: schema.clone(),
            ipc_fields,
            block_offsets: meta + data + 8,
            dictionary_blocks: vec![],
            record_blocks: vec![],
            finished: false,
            dictionary_tracker: DictionaryTracker::new(true),
        })
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    /// Writes [`RecordBatch`] to the file
    pub fn write(&mut self, batch: &RecordBatch, ipc_fields: Option<&[IpcField]>) -> Result<()> {
        if self.finished {
            return Err(ArrowError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Cannot write to a finished file".to_string(),
            )));
        }

        let ipc_fields = if let Some(ipc_fields) = ipc_fields {
            ipc_fields
        } else {
            self.ipc_fields.as_ref()
        };

        let columns = batch.clone().into();
        let (encoded_dictionaries, encoded_message) = encode_columns(
            &columns,
            ipc_fields,
            &mut self.dictionary_tracker,
            &self.options,
        )?;

        for encoded_dictionary in encoded_dictionaries {
            let (meta, data) = write_message(&mut self.writer, encoded_dictionary)?;

            let block = ipc::File::Block::new(self.block_offsets as i64, meta as i32, data as i64);
            self.dictionary_blocks.push(block);
            self.block_offsets += meta + data;
        }

        let (meta, data) = write_message(&mut self.writer, encoded_message)?;
        // add a record block for the footer
        let block = ipc::File::Block::new(
            self.block_offsets as i64,
            meta as i32, // TODO: is this still applicable?
            data as i64,
        );
        self.record_blocks.push(block);
        self.block_offsets += meta + data;
        Ok(())
    }

    /// Write footer and closing tag, then mark the writer as done
    pub fn finish(&mut self) -> Result<()> {
        // write EOS
        write_continuation(&mut self.writer, 0)?;

        let mut fbb = FlatBufferBuilder::new();
        let dictionaries = fbb.create_vector(&self.dictionary_blocks);
        let record_batches = fbb.create_vector(&self.record_blocks);
        let schema = schema::schema_to_fb_offset(&mut fbb, &self.schema, &self.ipc_fields);

        let root = {
            let mut footer_builder = ipc::File::FooterBuilder::new(&mut fbb);
            footer_builder.add_version(ipc::Schema::MetadataVersion::V5);
            footer_builder.add_schema(schema);
            footer_builder.add_dictionaries(dictionaries);
            footer_builder.add_recordBatches(record_batches);
            footer_builder.finish()
        };
        fbb.finish(root, None);
        let footer_data = fbb.finished_data();
        self.writer.write_all(footer_data)?;
        self.writer
            .write_all(&(footer_data.len() as i32).to_le_bytes())?;
        self.writer.write_all(&ARROW_MAGIC)?;
        self.writer.flush()?;
        self.finished = true;

        Ok(())
    }
}
