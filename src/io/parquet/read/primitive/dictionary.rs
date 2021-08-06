use std::sync::Arc;

use parquet2::{
    encoding::{bitpacking, hybrid_rle, uleb128},
    page::{DataPage, DataPageHeader, PrimitivePageDict},
    read::{levels, StreamingIterator},
    schema::Encoding,
    types::NativeType,
};

use super::super::utils as other_utils;
use super::{ColumnChunkMetaData, ColumnDescriptor};
use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    bitmap::{utils::BitmapIter, MutableBitmap},
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
    types::NativeType as ArrowNativeType,
};

fn read_dict_optional<K, T, A, F>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &PrimitivePageDict<T>,
    indices: &mut MutableBuffer<K>,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    K: DictionaryKey,
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let dict_values = dict.values();
    values.extend_from_trusted_len_iter(dict_values.iter().map(|x| op(*x)));

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let (_, consumed) = uleb128::decode(indices_buffer);
    let indices_buffer = &indices_buffer[consumed..];

    let non_null_indices_len = indices_buffer.len() * 8 / bit_width as usize;

    let mut new_indices = bitpacking::Decoder::new(indices_buffer, bit_width, non_null_indices_len);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                let remaining = additional - indices.len();
                let len = std::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    let value = if is_valid {
                        K::from_u32(new_indices.next().unwrap()).unwrap()
                    } else {
                        K::default()
                    };
                    indices.push(value);
                }
                validity.extend_from_slice(packed, 0, len);
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let index = K::from_u32(new_indices.next().unwrap()).unwrap();
                        indices.push(index)
                    })
                } else {
                    values.extend_constant(additional, A::default())
                }
            }
        }
    }
}

fn extend_from_page<K, T, A, F>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    indices: &mut MutableBuffer<K>,
    values: &mut MutableBuffer<A>,
    validity: &mut MutableBitmap,
    op: F,
) -> Result<()>
where
    K: DictionaryKey,
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let additional = page.num_values();

    assert_eq!(descriptor.max_rep_level(), 0);
    let is_optional = descriptor.max_def_level() == 1;
    match page.header() {
        DataPageHeader::V1(header) => {
            assert_eq!(header.definition_level_encoding, Encoding::Rle);

            let (_, validity_buffer, values_buffer) =
                levels::split_buffer_v1(page.buffer(), false, is_optional);

            match (&page.encoding(), page.dictionary_page(), is_optional) {
                (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
                    read_dict_optional(
                        validity_buffer,
                        values_buffer,
                        additional,
                        dict.as_any().downcast_ref().unwrap(),
                        indices,
                        values,
                        validity,
                        op,
                    )
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
                    read_dict_optional(
                        validity_buffer,
                        values_buffer,
                        additional,
                        dict.as_any().downcast_ref().unwrap(),
                        indices,
                        values,
                        validity,
                        op,
                    )
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

pub fn iter_to_array<K, T, A, I, E, F>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
    op: F,
) -> Result<Box<dyn Array>>
where
    ArrowError: From<E>,
    T: NativeType,
    K: DictionaryKey,
    E: Clone,
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: StreamingIterator<Item = std::result::Result<DataPage, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut indices = MutableBuffer::<K>::with_capacity(capacity);
    let mut values = MutableBuffer::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next() {
        extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            &mut indices,
            &mut values,
            &mut validity,
            op,
        )?
    }

    let keys = PrimitiveArray::from_data(K::DATA_TYPE, indices.into(), validity.into());
    let data_type = DictionaryArray::<K>::get_child(&data_type).clone();
    let values = Arc::new(PrimitiveArray::from_data(data_type, values.into(), None));
    Ok(Box::new(DictionaryArray::<K>::from_data(keys, values)))
}
