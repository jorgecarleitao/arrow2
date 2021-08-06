use crate::bitmap::Bitmap;

use parquet2::{
    compression::create_codec,
    encoding::{hybrid_rle::encode_bool, Encoding},
    metadata::ColumnDescriptor,
    page::{CompressedDataPage, DataPageHeader, DataPageHeaderV1, DataPageHeaderV2},
    schema::CompressionCodec,
    statistics::ParquetStatistics,
    write::WriteOptions,
};

use crate::error::Result;

use super::Version;

fn encode_iter_v1<I: Iterator<Item = bool>>(buffer: &mut Vec<u8>, iter: I) -> Result<()> {
    buffer.extend_from_slice(&[0; 4]);
    let start = buffer.len();
    encode_bool(buffer, iter)?;
    let end = buffer.len();
    let length = end - start;

    // write the first 4 bytes as length
    let length = (length as i32).to_le_bytes();
    (0..4).for_each(|i| buffer[start - 4 + i] = length[i]);
    Ok(())
}

fn encode_iter_v2<I: Iterator<Item = bool>>(writer: &mut Vec<u8>, iter: I) -> Result<()> {
    Ok(encode_bool(writer, iter)?)
}

fn encode_iter<I: Iterator<Item = bool>>(
    writer: &mut Vec<u8>,
    iter: I,
    version: Version,
) -> Result<()> {
    match version {
        Version::V1 => encode_iter_v1(writer, iter),
        Version::V2 => encode_iter_v2(writer, iter),
    }
}

/// writes the def levels to a `Vec<u8>` and returns it.
pub fn write_def_levels(
    writer: &mut Vec<u8>,
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
    repetition_levels_byte_length: usize,
    definition_levels_byte_length: usize,
    statistics: Option<ParquetStatistics>,
    descriptor: ColumnDescriptor,
    options: WriteOptions,
    encoding: Encoding,
) -> Result<CompressedDataPage> {
    match options.version {
        Version::V1 => {
            let header = DataPageHeader::V1(DataPageHeaderV1 {
                num_values: len as i32,
                encoding,
                definition_level_encoding: Encoding::Rle,
                repetition_level_encoding: Encoding::Rle,
                statistics,
            });

            Ok(CompressedDataPage::new(
                header,
                buffer,
                options.compression,
                uncompressed_page_size,
                None,
                descriptor,
            ))
        }
        Version::V2 => {
            let header = DataPageHeader::V2(DataPageHeaderV2 {
                num_values: len as i32,
                encoding,
                num_nulls: null_count as i32,
                num_rows: len as i32,
                definition_levels_byte_length: definition_levels_byte_length as i32,
                repetition_levels_byte_length: repetition_levels_byte_length as i32,
                is_compressed: Some(options.compression != CompressionCodec::Uncompressed),
                statistics,
            });

            Ok(CompressedDataPage::new(
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
    levels_byte_length: usize,
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
                codec.compress(&buffer[levels_byte_length..], &mut tmp)?;
                buffer.truncate(levels_byte_length);
                buffer.extend_from_slice(&tmp);
                buffer
            }
        }
    } else {
        buffer
    })
}

/// Auxiliary iterator adapter to declare the size hint of an iterator.
pub(super) struct ExactSizedIter<T, I: Iterator<Item = T>> {
    iter: I,
    remaining: usize,
}

impl<T, I: Iterator<Item = T> + Clone> Clone for ExactSizedIter<T, I> {
    fn clone(&self) -> Self {
        Self {
            iter: self.iter.clone(),
            remaining: self.remaining,
        }
    }
}

impl<T, I: Iterator<Item = T>> ExactSizedIter<T, I> {
    pub fn new(iter: I, length: usize) -> Self {
        Self {
            iter,
            remaining: length,
        }
    }
}

impl<T, I: Iterator<Item = T>> Iterator for ExactSizedIter<T, I> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            self.remaining -= 1;
            x
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// Returns the number of bits needed to bitpack `max`
#[inline]
pub fn get_bit_width(max: u64) -> u32 {
    64 - max.leading_zeros()
}
