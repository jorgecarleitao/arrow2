use std::convert::TryInto;

use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnDescriptor;
use parquet2::page::{split_buffer as _split_buffer, DataPage, DataPageHeader};

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
        let length = u32::from_le_bytes(self.values[0..4].try_into().unwrap()) as usize;
        self.values = &self.values[4..];
        let result = &self.values[..length];
        self.values = &self.values[length..];
        Some(result)
    }
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

pub fn split_buffer<'a>(
    page: &'a DataPage,
    descriptor: &ColumnDescriptor,
) -> (&'a [u8], &'a [u8], &'a [u8], &'static str) {
    let (rep_levels, validity_buffer, values_buffer) = _split_buffer(page, descriptor);

    let version = match page.header() {
        DataPageHeader::V1(_) => "V1",
        DataPageHeader::V2(_) => "V2",
    };
    (rep_levels, validity_buffer, values_buffer, version)
}
