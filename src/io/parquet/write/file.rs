use std::io::Write;

use parquet2::metadata::SchemaDescriptor;
use parquet2::write::RowGroupIter;
use parquet2::{metadata::KeyValue, write::WriteOptions};

use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};

use super::{schema::schema_to_metadata_key, to_parquet_schema};

/// Attaches [`Schema`] to `key_value_metadata`
pub fn add_arrow_schema(
    schema: &Schema,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Option<Vec<KeyValue>> {
    key_value_metadata
        .map(|mut x| {
            x.push(schema_to_metadata_key(schema));
            x
        })
        .or_else(|| Some(vec![schema_to_metadata_key(schema)]))
}

/// An interface to write a parquet to a [`Write`]
pub struct FileWriter<W: Write> {
    writer: parquet2::write::FileWriter<W>,
    schema: Schema,
}

// Accessors
impl<W: Write> FileWriter<W> {
    /// The options assigned to the file
    pub fn options(&self) -> &WriteOptions {
        self.writer.options()
    }

    /// The [`SchemaDescriptor`] assigned to this file
    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        self.writer.schema()
    }

    /// The [`Schema`] assigned to this file
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<W: Write> FileWriter<W> {
    /// Returns a new [`FileWriter`].
    /// # Error
    /// If it is unable to derive a parquet schema from [`Schema`].
    pub fn try_new(writer: W, schema: Schema, options: WriteOptions) -> Result<Self> {
        let parquet_schema = to_parquet_schema(&schema)?;

        let created_by = Some("Arrow2 - Native Rust implementation of Arrow".to_string());

        Ok(Self {
            writer: parquet2::write::FileWriter::new(writer, parquet_schema, options, created_by),
            schema,
        })
    }

    /// Writes the header of the file
    pub fn start(&mut self) -> Result<()> {
        Ok(self.writer.start()?)
    }

    /// Writes a row group to the file.
    pub fn write(&mut self, row_group: RowGroupIter<'_, ArrowError>) -> Result<()> {
        Ok(self.writer.write(row_group)?)
    }

    /// Writes the footer of the parquet file. Returns the total size of the file.
    pub fn end(self, key_value_metadata: Option<Vec<KeyValue>>) -> Result<(u64, W)> {
        let key_value_metadata = add_arrow_schema(&self.schema, key_value_metadata);
        Ok(self.writer.end(key_value_metadata)?)
    }
}
