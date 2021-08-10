use parquet2::{
    encoding::{bitpacking, hybrid_rle, uleb128, Encoding},
    page::{DataPage, DataPageHeader, DataPageHeaderExt, PrimitivePageDict},
    read::levels,
    types::NativeType,
};

use super::super::utils as other_utils;
use super::utils::ExactChunksIter;
use super::ColumnDescriptor;
use crate::{
    bitmap::{utils::BitmapIter, MutableBitmap},
    buffer::MutableBuffer,
    error::Result,
    types::NativeType as ArrowNativeType,
};

fn read_dict_buffer_optional<T, A, F>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &PrimitivePageDict<T>,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let dict_values = dict.values();

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let (_, consumed) = uleb128::decode(indices_buffer);
    let indices_buffer = &indices_buffer[consumed..];

    let non_null_indices_len = indices_buffer.len() * 8 / bit_width as usize;

    let mut indices = bitpacking::Decoder::new(indices_buffer, bit_width, non_null_indices_len);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                let remaining = additional - values.len();
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    let value = if is_valid {
                        op(dict_values[indices.next().unwrap() as usize])
                    } else {
                        A::default()
                    };
                    values.push(value);
                }
                validity.extend_from_slice(packed, 0, len);
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let index = indices.next().unwrap() as usize;
                        let value = op(dict_values[index]);
                        values.push(value)
                    })
                } else {
                    values.extend_constant(additional, A::default())
                }
            }
        }
    }
}

fn read_nullable<T, A, F>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let mut chunks = ExactChunksIter::<T>::new(values_buffer);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // the pack may contain more items than needed.
                let remaining = additional - values.len();
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    let value = if is_valid {
                        op(chunks.next().unwrap())
                    } else {
                        A::default()
                    };
                    values.push(value);
                }
                validity.extend_from_slice(packed, 0, len);
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let value = op(chunks.next().unwrap());
                        values.push(value)
                    })
                } else {
                    values.extend_constant(additional, A::default())
                }
            }
        }
    }
}

fn read_required<T, A, F>(
    values_buffer: &[u8],
    additional: usize,
    values: &mut MutableBuffer<A>,
    op: F,
) where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    assert_eq!(values_buffer.len(), additional * std::mem::size_of::<T>());
    let iterator = ExactChunksIter::<T>::new(values_buffer);

    let iterator = iterator.map(|value| op(value));

    values.extend_from_trusted_len_iter(iterator);
}

pub fn extend_from_page<T, A, F>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
    op: F,
) -> Result<()>
where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let additional = page.num_values();

    assert_eq!(descriptor.max_rep_level(), 0);
    let is_optional = descriptor.max_def_level() == 1;
    match page.header() {
        DataPageHeader::V1(header) => {
            assert_eq!(header.definition_level_encoding(), Encoding::Rle);

            let (_, validity_buffer, values_buffer) =
                levels::split_buffer_v1(page.buffer(), false, is_optional);

            match (&page.encoding(), page.dictionary_page(), is_optional) {
                (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
                    read_dict_buffer_optional(
                        validity_buffer,
                        values_buffer,
                        additional,
                        dict.as_any().downcast_ref().unwrap(),
                        values,
                        validity,
                        op,
                    )
                }
                (Encoding::Plain, None, true) => read_nullable(
                    validity_buffer,
                    values_buffer,
                    additional,
                    values,
                    validity,
                    op,
                ),
                (Encoding::Plain, None, false) => {
                    read_required(page.buffer(), additional, values, op)
                }
                _ => {
                    return Err(other_utils::not_implemented(
                        &page.encoding(),
                        is_optional,
                        page.dictionary_page().is_some(),
                        "V1",
                        "primitive",
                    ))
                }
            }
        }
        DataPageHeader::V2(header) => {
            let def_level_buffer_length = header.definition_levels_byte_length as usize;

            let (_, validity_buffer, values_buffer) =
                levels::split_buffer_v2(page.buffer(), 0, def_level_buffer_length);
            match (&page.encoding(), page.dictionary_page(), is_optional) {
                (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
                    read_dict_buffer_optional(
                        validity_buffer,
                        values_buffer,
                        additional,
                        dict.as_any().downcast_ref().unwrap(),
                        values,
                        validity,
                        op,
                    )
                }
                (Encoding::Plain, None, true) => read_nullable(
                    validity_buffer,
                    values_buffer,
                    additional,
                    values,
                    validity,
                    op,
                ),
                (Encoding::Plain, None, false) => {
                    read_required::<T, A, F>(page.buffer(), additional, values, op)
                }
                _ => {
                    return Err(other_utils::not_implemented(
                        &page.encoding(),
                        is_optional,
                        page.dictionary_page().is_some(),
                        "V2",
                        "primitive",
                    ))
                }
            }
        }
    };
    Ok(())
}
