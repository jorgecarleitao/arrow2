//! APIs to write to JSON
mod format;
mod serialize;
pub use fallible_streaming_iterator::*;
pub use format::*;
pub use serialize::serialize;

use crate::{
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};

/// Writes blocks of JSON-encoded data into `writer`, ensuring that the written
/// JSON has the expected `format`
pub fn write<W, F, I>(writer: &mut W, format: F, mut blocks: I) -> Result<()>
where
    W: std::io::Write,
    F: JsonFormat,
    I: FallibleStreamingIterator<Item = [u8], Error = ArrowError>,
{
    format.start_stream(writer)?;
    let mut is_first_row = true;
    while let Some(block) = blocks.next()? {
        format.start_row(writer, is_first_row)?;
        is_first_row = false;
        writer.write_all(block)?;
    }
    format.end_stream(writer)?;
    Ok(())
}

/// [`FallibleStreamingIterator`] that serializes a [`RecordBatch`] to bytes.
/// Advancing it is CPU-bounded
pub struct Serializer<F: JsonFormat, I: Iterator<Item = Result<RecordBatch>>> {
    iter: I,
    buffer: Vec<u8>,
    format: F,
}

impl<F: JsonFormat, I: Iterator<Item = Result<RecordBatch>>> Serializer<F, I> {
    /// Creates a new [`Serializer`].
    pub fn new(iter: I, buffer: Vec<u8>, format: F) -> Self {
        Self {
            iter,
            buffer,
            format,
        }
    }
}

impl<F: JsonFormat, I: Iterator<Item = Result<RecordBatch>>> FallibleStreamingIterator
    for Serializer<F, I>
{
    type Item = [u8];

    type Error = ArrowError;

    fn advance(&mut self) -> Result<()> {
        self.buffer.clear();
        self.iter
            .next()
            .map(|maybe_batch| {
                maybe_batch.map(|batch| {
                    let names = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>();
                    serialize(&names, batch.columns(), self.format, &mut self.buffer)
                })
            })
            .transpose()?;
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        if !self.buffer.is_empty() {
            Some(&self.buffer)
        } else {
            None
        }
    }
}
