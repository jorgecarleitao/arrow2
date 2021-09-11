use std::sync::Arc;

use parquet2::{
    encoding::{hybrid_rle, Encoding},
    metadata::{ColumnChunkMetaData, ColumnDescriptor},
    page::{BinaryPageDict, DataPage},
    read::StreamingIterator,
};

use super::super::utils as other_utils;
use crate::{
    array::{Array, DictionaryArray, DictionaryKey, Offset, PrimitiveArray, Utf8Array},
    bitmap::{utils::BitmapIter, MutableBitmap},
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
};

#[allow(clippy::too_many_arguments)]
fn read_dict_optional<K, O>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &BinaryPageDict,
    indices: &mut MutableBuffer<K>,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) where
    K: DictionaryKey,
    O: Offset,
{
    let length = indices.len() + additional;
    values.extend_from_slice(dict.values());
    offsets.extend_from_trusted_len_iter(
        dict.offsets()
            .iter()
            .map(|x| O::from_usize(*x as usize).unwrap()),
    );

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let mut new_indices =
        hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);

    let validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                let remaining = length - indices.len();
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
                    indices.extend_constant(additional, *indices.last().unwrap())
                }
            }
        }
    }
}

fn extend_from_page<K, O>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    indices: &mut MutableBuffer<K>,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) -> Result<()>
where
    K: DictionaryKey,
    O: Offset,
{
    let additional = page.num_values();

    assert_eq!(descriptor.max_rep_level(), 0);
    let is_optional = descriptor.max_def_level() == 1;

    let (_, validity_buffer, values_buffer, version) = other_utils::split_buffer(page, descriptor);

    match (&page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
            read_dict_optional(
                validity_buffer,
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                indices,
                offsets,
                values,
                validity,
            )
        }
        _ => {
            return Err(other_utils::not_implemented(
                &page.encoding(),
                is_optional,
                page.dictionary_page().is_some(),
                version,
                "primitive",
            ))
        }
    }
    Ok(())
}

pub fn iter_to_array<K, O, I, E>(
    mut iter: I,
    metadata: &ColumnChunkMetaData,
    data_type: DataType,
) -> Result<Box<dyn Array>>
where
    ArrowError: From<E>,
    O: Offset,
    K: DictionaryKey,
    E: Clone,
    I: StreamingIterator<Item = std::result::Result<DataPage, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut indices = MutableBuffer::<K>::with_capacity(capacity);
    let mut values = MutableBuffer::<u8>::with_capacity(0);
    let mut offsets = MutableBuffer::<O>::with_capacity(1 + capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next() {
        extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            &mut indices,
            &mut offsets,
            &mut values,
            &mut validity,
        )?
    }

    if offsets.len() == 0 {
        // the array is empty and thus we need to push the first offset ourselves.
        offsets.push(O::zero());
    };
    let keys = PrimitiveArray::from_data(K::DATA_TYPE, indices.into(), validity.into());
    let data_type = DictionaryArray::<K>::get_child(&data_type).clone();
    let values = Arc::new(Utf8Array::from_data(
        data_type,
        offsets.into(),
        values.into(),
        None,
    ));
    Ok(Box::new(DictionaryArray::<K>::from_data(keys, values)))
}
