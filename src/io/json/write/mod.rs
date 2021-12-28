//! APIs to write to JSON
mod format;
mod serialize;
pub use fallible_streaming_iterator::*;
pub use format::*;
pub use serialize::serialize;

use crate::{
    array::Array,
    columns::Columns,
    error::{ArrowError, Result},
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

/// [`FallibleStreamingIterator`] that serializes a [`Columns`] to bytes.
/// Advancing it is CPU-bounded
pub struct Serializer<F, A, I>
where
    F: JsonFormat,
    A: std::borrow::Borrow<dyn Array>,
    I: Iterator<Item = Result<Columns<A>>>,
{
    batches: I,
    names: Vec<String>,
    buffer: Vec<u8>,
    format: F,
}

impl<F, A, I> Serializer<F, A, I>
where
    F: JsonFormat,
    A: std::borrow::Borrow<dyn Array>,
    I: Iterator<Item = Result<Columns<A>>>,
{
    /// Creates a new [`Serializer`].
    pub fn new(batches: I, names: Vec<String>, buffer: Vec<u8>, format: F) -> Self {
        Self {
            batches,
            names,
            buffer,
            format,
        }
    }
}

impl<F, A, I> FallibleStreamingIterator for Serializer<F, A, I>
where
    F: JsonFormat,
    A: std::borrow::Borrow<dyn Array>,
    I: Iterator<Item = Result<Columns<A>>>,
{
    type Item = [u8];

    type Error = ArrowError;

    fn advance(&mut self) -> Result<()> {
        self.buffer.clear();
        self.batches
            .next()
            .map(|maybe_columns| {
                maybe_columns
                    .map(|columns| serialize(&self.names, &columns, self.format, &mut self.buffer))
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
