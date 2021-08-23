use parquet2::encoding::{get_length, Encoding};
use parquet2::{
    page::{DataPage, DataPageHeader, DataPageHeaderExt},
    read::levels,
};

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

pub fn split_buffer(page: &DataPage, is_optional: bool) -> (&[u8], &[u8], &'static str) {
    match page.header() {
        DataPageHeader::V1(header) => {
            assert_eq!(header.definition_level_encoding(), Encoding::Rle);

            let (_, validity_buffer, values_buffer) =
                levels::split_buffer_v1(page.buffer(), false, is_optional);
            (validity_buffer, values_buffer, "V1")
        }
        DataPageHeader::V2(header) => {
            let def_level_buffer_length = header.definition_levels_byte_length as usize;

            let (_, validity_buffer, values_buffer) =
                levels::split_buffer_v2(page.buffer(), 0, def_level_buffer_length);
            (validity_buffer, values_buffer, "V2")
        }
    }
}
