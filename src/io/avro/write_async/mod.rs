//! Async write Avro
mod block;
pub use block::write_block;

use avro_schema::{Field as AvroField, Record, Schema as AvroSchema};
use futures::{AsyncWrite, AsyncWriteExt};

use crate::error::Result;

use super::{
    write::{write_schema, AVRO_MAGIC, SYNC_NUMBER},
    Compression,
};

/// Writes Avro's metadata to `writer`.
pub async fn write_metadata<W>(
    writer: &mut W,
    fields: Vec<AvroField>,
    compression: Option<Compression>,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&AVRO_MAGIC).await?;

    // * file metadata, including the schema.
    let schema = AvroSchema::Record(Record::new("", fields));

    let mut scratch = vec![];
    write_schema(&mut scratch, &schema, compression)?;

    writer.write_all(&scratch).await?;

    // The 16-byte, randomly-generated sync marker for this file.
    writer.write_all(&SYNC_NUMBER).await?;

    Ok(())
}
