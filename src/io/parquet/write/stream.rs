//! Contains `async` APIs to write to parquet.
use futures::AsyncWrite;

use parquet2::metadata::{KeyValue, SchemaDescriptor};
use parquet2::write::RowGroupIter;

use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use super::file::add_arrow_schema;
use super::{to_parquet_schema, WriteOptions};

/// An interface to write a parquet to a [`AsyncWrite`]
pub struct FileStreamer<W: AsyncWrite + Unpin + Send> {
    writer: parquet2::write::FileStreamer<W>,
    schema: Schema,
}

// Accessors
impl<W: AsyncWrite + Unpin + Send> FileStreamer<W> {
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

impl<W: AsyncWrite + Unpin + Send> FileStreamer<W> {
    /// Returns a new [`FileStreamer`].
    /// # Error
    /// If it is unable to derive a parquet schema from [`Schema`].
    pub fn try_new(writer: W, schema: Schema, options: WriteOptions) -> Result<Self> {
        let parquet_schema = to_parquet_schema(&schema)?;

        let created_by = Some("Arrow2 - Native Rust implementation of Arrow".to_string());

        Ok(Self {
            writer: parquet2::write::FileStreamer::new(writer, parquet_schema, options, created_by),
            schema,
        })
    }

    /// Writes the header of the file
    pub async fn start(&mut self) -> Result<()> {
        Ok(self.writer.start().await?)
    }

    /// Writes a row group to the file.
    pub async fn write(
        &mut self,
        row_group: RowGroupIter<'_, ArrowError>,
        num_rows: usize,
    ) -> Result<()> {
        Ok(self.writer.write(row_group, num_rows).await?)
    }

    /// Writes the footer of the parquet file. Returns the total size of the file.
    pub async fn end(self, key_value_metadata: Option<Vec<KeyValue>>) -> Result<(u64, W)> {
        let key_value_metadata = add_arrow_schema(&self.schema, key_value_metadata);
        Ok(self.writer.end(key_value_metadata).await?)
    }
}
