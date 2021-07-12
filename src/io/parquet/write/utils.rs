use std::io::{Seek, SeekFrom, Write};

use crate::bitmap::Bitmap;

use parquet2::{
    compression::create_codec,
    encoding::{hybrid_rle::encode, Encoding},
    metadata::ColumnDescriptor,
    read::{CompressedPage, PageHeader},
    schema::{CompressionCodec, DataPageHeader, DataPageHeaderV2},
    statistics::ParquetStatistics,
    write::WriteOptions,
};

use crate::error::Result;

use super::Version;

#[inline]
fn encode_iter_v1<W: Write + Seek, I: Iterator<Item = bool>>(
    writer: &mut W,
    iter: I,
) -> Result<()> {
    writer.write_all(&[0; 4])?;
    let start = writer.stream_position()?;
    encode(writer, iter)?;
    let end = writer.stream_position()?;
    let length = end - start;

    // write the first 4 bytes as length
    writer.seek(SeekFrom::Current(-(length as i64) - 4))?;
    writer.write_all((length as i32).to_le_bytes().as_ref())?;

    // return to the last position
    writer.seek(SeekFrom::Current(end as i64))?;
    Ok(())
}

#[inline]
fn encode_iter_v2<W: Write + Seek, I: Iterator<Item = bool>>(
    writer: &mut W,
    iter: I,
) -> Result<()> {
    Ok(encode(writer, iter)?)
}

fn encode_iter<W: Write + Seek, I: Iterator<Item = bool>>(
    writer: &mut W,
    iter: I,
    version: Version,
) -> Result<()> {
    match version {
        Version::V1 => encode_iter_v1(writer, iter),
        Version::V2 => encode_iter_v2(writer, iter),
    }
}

/// writes the def levels to a `Vec<u8>` and returns it.
#[inline]
pub fn write_def_levels<W: Write + Seek>(
    writer: &mut W,
    is_optional: bool,
    validity: &Option<Bitmap>,
    len: usize,
    version: Version,
) -> Result<()> {
    // encode def levels
    match (is_optional, validity) {
        (true, Some(validity)) => encode_iter(writer, validity.iter(), version),
        (true, None) => encode_iter(writer, std::iter::repeat(true).take(len), version),
        _ => Ok(()), // is required => no def levels
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
