//! APIs to write to JSON
mod serialize;
mod utf8;

pub use fallible_streaming_iterator::*;
pub(crate) use serialize::new_serializer;
use serialize::serialize;

use crate::{array::Array, error::Error};

/// [`FallibleStreamingIterator`] that serializes an [`Array`] to bytes of valid JSON
/// # Implementation
/// Advancing this iterator CPU-bounded
#[derive(Debug, Clone)]
pub struct Serializer<A, I>
where
    A: AsRef<dyn Array>,
    I: Iterator<Item = Result<A, Error>>,
{
    arrays: I,
    buffer: Vec<u8>,
}

impl<A, I> Serializer<A, I>
where
    A: AsRef<dyn Array>,
    I: Iterator<Item = Result<A, Error>>,
{
    /// Creates a new [`Serializer`].
    pub fn new(arrays: I, buffer: Vec<u8>) -> Self {
        Self { arrays, buffer }
    }
}

impl<A, I> FallibleStreamingIterator for Serializer<A, I>
where
    A: AsRef<dyn Array>,
    I: Iterator<Item = Result<A, Error>>,
{
    type Item = [u8];

    type Error = Error;

    fn advance(&mut self) -> Result<(), Error> {
        self.buffer.clear();
        self.arrays
            .next()
            .map(|maybe_array| maybe_array.map(|array| serialize(array.as_ref(), &mut self.buffer)))
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

/// Writes valid JSON from an iterator of (assumed JSON-encoded) bytes to `writer`
pub fn write<W, I>(writer: &mut W, mut blocks: I) -> Result<(), Error>
where
    W: std::io::Write,
    I: FallibleStreamingIterator<Item = [u8], Error = Error>,
{
    writer.write_all(&[b'['])?;
    let mut is_first_row = true;
    while let Some(block) = blocks.next()? {
        if !is_first_row {
            writer.write_all(&[b','])?;
        }
        is_first_row = false;
        writer.write_all(block)?;
    }
    writer.write_all(&[b']'])?;
    Ok(())
}
