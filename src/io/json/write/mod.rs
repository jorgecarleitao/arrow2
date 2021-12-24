//! APIs to write to JSON
mod format;
mod serialize;
pub use fallible_streaming_iterator::*;
pub use format::*;
pub use serialize::serialize;

use crate::{
    array::Array,
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

/// [`FallibleStreamingIterator`] that serializes a [`RecordBatch`] to bytes.
/// Advancing it is CPU-bounded
pub struct Serializer<F, A, I>
where
    F: JsonFormat,
    A: AsRef<dyn Array>,
    I: Iterator<Item = Result<Vec<A>>>,
{
    iter: I,
    names: Vec<String>,
    buffer: Vec<u8>,
    format: F,
}

impl<F, A, I> Serializer<F, A, I>
where
    F: JsonFormat,
    A: AsRef<dyn Array>,
    I: Iterator<Item = Result<Vec<A>>>,
{
    /// Creates a new [`Serializer`].
    pub fn new(iter: I, names: Vec<String>, buffer: Vec<u8>, format: F) -> Self {
        Self {
            iter,
            names,
            buffer,
            format,
        }
    }
}

impl<F, A, I> FallibleStreamingIterator for Serializer<F, A, I>
where
    F: JsonFormat,
    A: AsRef<dyn Array>,
    I: Iterator<Item = Result<Vec<A>>>,
{
    type Item = [u8];

    type Error = ArrowError;

    fn advance(&mut self) -> Result<()> {
        self.buffer.clear();
        self.iter
            .next()
            .map(|maybe_arrays| {
                maybe_arrays
                    .map(|arrays| serialize(&self.names, &arrays, self.format, &mut self.buffer))
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
