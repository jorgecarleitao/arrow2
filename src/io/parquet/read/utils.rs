use std::convert::TryInto;

use parquet2::encoding::{hybrid_rle, Encoding};
use parquet2::metadata::ColumnDescriptor;
use parquet2::page::{split_buffer as _split_buffer, DataPage, DataPageHeader};

use crate::array::DictionaryKey;
use crate::bitmap::utils::BitmapIter;
use crate::bitmap::MutableBitmap;
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

/// A private trait representing structs that can receive elements.
pub(super) trait Pushable<T: Default> {
    fn reserve(&mut self, additional: usize);
    fn push(&mut self, value: T);
    #[inline]
    fn push_null(&mut self) {
        self.push(T::default())
    }
    fn extend_constant(&mut self, additional: usize, value: T);
}

impl Pushable<bool> for MutableBitmap {
    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.reserve(additional)
    }

    #[inline]
    fn push(&mut self, value: bool) {
        self.push(value)
    }

    #[inline]
    fn extend_constant(&mut self, additional: usize, value: bool) {
        self.extend_constant(additional, value)
    }
}

impl<A: Copy + Default> Pushable<A> for Vec<A> {
    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.reserve(additional)
    }

    #[inline]
    fn push(&mut self, value: A) {
        self.push(value)
    }

    #[inline]
    fn extend_constant(&mut self, additional: usize, value: A) {
        self.resize(self.len() + additional, value);
    }
}

/// Extends a [`Pushable`] from an iterator of non-null values and an hybrid-rle decoder
#[inline]
pub(super) fn extend_from_decoder<'a, T: Default, C: Pushable<T>, I: Iterator<Item = T>>(
    validity: &mut MutableBitmap,
    decoder: &mut hybrid_rle::Decoder<'a>,
    page_length: usize, // data page length
    values: &mut C,
    mut values_iter: I,
) {
    let remaining = page_length;
    for run in decoder {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(pack) => {
                // compute the length of the pack
                let pack_size = pack.len() * 8;
                let pack_remaining = page_length;
                let length = std::cmp::min(pack_size, pack_remaining);

                let additional = remaining.min(length);

                // extend validity
                validity.extend_from_slice(pack, 0, additional);

                // extend values
                let iter = BitmapIter::new(pack, 0, additional);
                for is_valid in iter {
                    if is_valid {
                        values.push(values_iter.next().unwrap())
                    } else {
                        values.push_null()
                    };
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, length) => {
                let is_set = value[0] == 1;

                // extend validity
                let length = length;
                let additional = remaining.min(length);
                validity.extend_constant(additional, is_set);

                // extend values
                if is_set {
                    (0..additional).for_each(|_| values.push(values_iter.next().unwrap()));
                } else {
                    values.extend_constant(additional, T::default());
                }
            }
        }
    }
}

pub(super) fn read_dict_optional<K>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    indices: &mut Vec<K>,
    validity: &mut MutableBitmap,
) where
    K: DictionaryKey,
{
    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let new_indices =
        hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);
    let indices_iter = new_indices.map(|x| K::from_u32(x).unwrap());

    let mut validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    extend_from_decoder(
        validity,
        &mut validity_iterator,
        additional,
        indices,
        indices_iter,
    )
}
