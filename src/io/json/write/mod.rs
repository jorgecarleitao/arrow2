//! APIs to write to JSON
mod format;
mod serialize;

pub use fallible_streaming_iterator::*;
pub use serialize::serialize;

use crate::{
    array::Array,
    chunk::Chunk,
    error::{ArrowError, Result},
};
use format::*;

/// The supported variations of JSON supported
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Format {
    /// JSON
    Json,
    /// NDJSON (http://ndjson.org/)
    NewlineDelimitedJson,
}

fn _write<W, F, I>(writer: &mut W, format: F, mut blocks: I) -> Result<()>
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

/// Writes blocks of JSON-encoded data into `writer` according to format [`Format`].
/// # Implementation
/// This is IO-bounded
pub fn write<W, I>(writer: &mut W, format: Format, blocks: I) -> Result<()>
where
    W: std::io::Write,
    I: FallibleStreamingIterator<Item = [u8], Error = ArrowError>,
{
    match format {
        Format::Json => _write(writer, JsonArray::default(), blocks),
        Format::NewlineDelimitedJson => _write(writer, LineDelimited::default(), blocks),
    }
}

/// [`FallibleStreamingIterator`] that serializes a [`Chunk`] to bytes.
/// Advancing it is CPU-bounded
pub struct Serializer<A, I>
where
    A: AsRef<dyn Array>,
    I: Iterator<Item = Result<Chunk<A>>>,
{
    batches: I,
    names: Vec<String>,
    buffer: Vec<u8>,
    format: Format,
}

impl<A, I> Serializer<A, I>
where
    A: AsRef<dyn Array>,
    I: Iterator<Item = Result<Chunk<A>>>,
{
    /// Creates a new [`Serializer`].
    pub fn new(batches: I, names: Vec<String>, buffer: Vec<u8>, format: Format) -> Self {
        Self {
            batches,
            names,
            buffer,
            format,
        }
    }
}

impl<A, I> FallibleStreamingIterator for Serializer<A, I>
where
    A: AsRef<dyn Array>,
    I: Iterator<Item = Result<Chunk<A>>>,
{
    type Item = [u8];

    type Error = ArrowError;

    fn advance(&mut self) -> Result<()> {
        self.buffer.clear();
        self.batches
            .next()
            .map(|maybe_chunk| {
                maybe_chunk
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
