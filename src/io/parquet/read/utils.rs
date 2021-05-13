use parquet2::{encoding::get_length, schema::Encoding};

use crate::error::ArrowError;

pub struct BinaryIter<'a> {
    values: &'a [u8],
}

impl<'a> BinaryIter<'a> {
    pub fn new(values: &'a [u8]) -> Self {
        Self { values }
    }
}

impl<'a> Iterator for BinaryIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            return None;
        }
        let length = get_length(self.values) as usize;
        self.values = &self.values[4..];
        let result = &self.values[..length];
        self.values = &self.values[length..];
        Some(result)
    }
}

#[inline]
pub fn split_buffer_v1(buffer: &[u8]) -> (&[u8], &[u8]) {
    let def_level_buffer_length = get_length(&buffer) as usize;
    (
        &buffer[4..4 + def_level_buffer_length],
        &buffer[4 + def_level_buffer_length..],
    )
}

pub fn split_buffer_v2(buffer: &[u8], def_level_buffer_length: usize) -> (&[u8], &[u8]) {
    (
        &buffer[..def_level_buffer_length],
        &buffer[def_level_buffer_length..],
    )
}

pub fn not_implemented(
    encoding: &Encoding,
    is_optional: bool,
    has_dict: bool,
    version: &str,
    physical_type: &str,
) -> ArrowError {
    let required = if is_optional { "optional" } else { "required" };
    let dict = if has_dict { ", dictionary-encoded" } else { "" };
    ArrowError::NotYetImplemented(format!(
        "Decoding \"{:?}\"-encoded{} {} {} pages is not yet implemented for {}",
        encoding, dict, required, version, physical_type
    ))
}
