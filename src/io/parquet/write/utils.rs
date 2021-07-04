use crate::bitmap::Bitmap;
use parquet2::compression::create_codec;
use parquet2::encoding::hybrid_rle::encode;

use parquet2::{
    encoding::Encoding,
    metadata::ColumnDescriptor,
    read::{CompressedPage, PageHeader},
    schema::{CompressionCodec, DataPageHeader, DataPageHeaderV2},
    statistics::ParquetStatistics,
    write::WriteOptions,
};

use crate::error::Result;

use super::Version;

#[inline]
fn encode_iter_v1<I: Iterator<Item = bool>>(iter: I) -> Result<Vec<u8>> {
    let mut buffer = std::io::Cursor::new(vec![0; 4]);
    buffer.set_position(4);
    encode(&mut buffer, iter)?;
    let mut buffer = buffer.into_inner();
    let length = buffer.len() - 4;
    // todo: pay this small debt (loop?)
    let length = length.to_le_bytes();
    buffer[0] = length[0];
    buffer[1] = length[1];
    buffer[2] = length[2];
    buffer[3] = length[3];
    Ok(buffer)
}

#[inline]
fn encode_iter_v2<I: Iterator<Item = bool>>(iter: I) -> Result<Vec<u8>> {
    let mut buffer = vec![];
    encode(&mut buffer, iter)?;
    Ok(buffer)
}

fn encode_iter<I: Iterator<Item = bool>>(iter: I, version: Version) -> Result<Vec<u8>> {
    match version {
        Version::V1 => encode_iter_v1(iter),
        Version::V2 => encode_iter_v2(iter),
    }
}

/// writes the def levels to a `Vec<u8>` and returns it.
/// Note that this function
#[inline]
pub fn write_def_levels(
    is_optional: bool,
    validity: &Option<Bitmap>,
    len: usize,
    version: Version,
) -> Result<Vec<u8>> {
    // encode def levels
    match (is_optional, validity) {
        (true, Some(validity)) => encode_iter(validity.iter(), version),
        (true, None) => encode_iter(std::iter::repeat(true).take(len), version),
        _ => Ok(vec![]), // is required => no def levels
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_plain_page(
    buffer: Vec<u8>,
    len: usize,
    null_count: usize,
    uncompressed_page_size: usize,
    definition_levels_byte_length: usize,
    statistics: Option<ParquetStatistics>,
    descriptor: ColumnDescriptor,
    options: WriteOptions,
) -> Result<CompressedPage> {
    match options.version {
        Version::V1 => {
            let header = PageHeader::V1(DataPageHeader {
                num_values: len as i32,
                encoding: Encoding::Plain,
                definition_level_encoding: Encoding::Rle,
                repetition_level_encoding: Encoding::Rle,
                statistics,
            });

            Ok(CompressedPage::new(
                header,
                buffer,
                options.compression,
                uncompressed_page_size,
                None,
                descriptor,
            ))
        }
        Version::V2 => {
            let header = PageHeader::V2(DataPageHeaderV2 {
                num_values: len as i32,
                encoding: Encoding::Plain,
                num_nulls: null_count as i32,
                num_rows: len as i32,
                definition_levels_byte_length: definition_levels_byte_length as i32,
                repetition_levels_byte_length: 0,
                is_compressed: Some(options.compression != CompressionCodec::Uncompressed),
                statistics,
            });

            Ok(CompressedPage::new(
                header,
                buffer,
                options.compression,
                uncompressed_page_size,
                None,
                descriptor,
            ))
        }
    }
}

pub fn compress(
    mut buffer: Vec<u8>,
    options: WriteOptions,
    definition_levels_byte_length: usize,
) -> Result<Vec<u8>> {
    let codec = create_codec(&options.compression)?;
    Ok(if let Some(mut codec) = codec {
        match options.version {
            Version::V1 => {
                // todo: remove this allocation by extending `buffer` directly.
                // needs refactoring `compress`'s API.
                let mut tmp = vec![];
                codec.compress(&buffer, &mut tmp)?;
                tmp
            }
            Version::V2 => {
                // todo: remove this allocation by extending `buffer` directly.
                // needs refactoring `compress`'s API.
                let mut tmp = vec![];
                codec.compress(&buffer[definition_levels_byte_length..], &mut tmp)?;
                buffer.truncate(definition_levels_byte_length);
                buffer.extend_from_slice(&tmp);
                buffer
            }
        }
    } else {
        buffer
    })
}
