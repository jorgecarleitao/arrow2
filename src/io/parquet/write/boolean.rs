use parquet2::{
    compression::create_codec,
    encoding::{hybrid_rle::bitpacked_encode, Encoding},
    read::{CompressedPage, PageV1},
    schema::{CompressionCodec, DataPageHeader},
};

use super::utils;
use crate::array::*;
use crate::error::Result;

pub fn array_to_page_v1(
    array: &BooleanArray,
    compression: CompressionCodec,
    is_optional: bool,
) -> Result<CompressedPage> {
    let validity = array.validity();

    let buffer = utils::write_def_levels(is_optional, validity, array.len())?;

    let iterator = array.iter().flatten().take(
        validity
            .as_ref()
            .map(|x| x.len() - x.null_count())
            .unwrap_or_else(|| array.len()),
    );

    // encode values using bitpacking
    let len = buffer.len();
    let mut buffer = std::io::Cursor::new(buffer);
    buffer.set_position(len as u64);
    bitpacked_encode(&mut buffer, iterator)?;
    let buffer = buffer.into_inner();

    let uncompressed_page_size = buffer.len();

    let codec = create_codec(&compression)?;
    let buffer = if let Some(mut codec) = codec {
        // todo: remove this allocation by extending `buffer` directly.
        // needs refactoring `compress`'s API.
        let mut tmp = vec![];
        codec.compress(&buffer, &mut tmp)?;
        tmp
    } else {
        buffer
    };

    let header = DataPageHeader {
        num_values: array.len() as i32,
        encoding: Encoding::Plain,
        definition_level_encoding: Encoding::Rle,
        repetition_level_encoding: Encoding::Rle,
        statistics: None,
    };

    Ok(CompressedPage::V1(PageV1 {
        buffer,
        header,
        compression,
        uncompressed_page_size,
        dictionary_page: None,
        statistics: None,
    }))
}
