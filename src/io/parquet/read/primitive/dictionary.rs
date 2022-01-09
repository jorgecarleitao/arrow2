use std::sync::Arc;

use parquet2::{
    encoding::{hybrid_rle, Encoding},
    page::{DataPage, PrimitivePageDict},
    types::NativeType,
    FallibleStreamingIterator,
};

use super::super::utils;
use super::{ColumnChunkMetaData, ColumnDescriptor};
use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    bitmap::{utils::BitmapIter, MutableBitmap},
    datatypes::DataType,
    error::{ArrowError, Result},
    types::NativeType as ArrowNativeType,
};

#[allow(clippy::too_many_arguments)]
fn read_dict_optional<K, T, A, F>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &PrimitivePageDict<T>,
    indices: &mut Vec<K>,
    values: &mut Vec<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    K: DictionaryKey,
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let dict_values = dict.values();
    values.extend(dict_values.iter().map(|x| op(*x)));

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
                    values.resize(values.len() + additional, A::default());
                }
            }
        }
    }
}

fn extend_from_page<K, T, A, F>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    indices: &mut Vec<K>,
    values: &mut Vec<A>,
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

    let (_, validity_buffer, values_buffer, version) = utils::split_buffer(page, descriptor);

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
            return Err(utils::not_implemented(
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
    A: ArrowNativeType,
    F: Copy + Fn(T) -> A,
    I: FallibleStreamingIterator<Item = DataPage, Error = E>,
{
    let capacity = metadata.num_values() as usize;
    let mut indices = Vec::<K>::with_capacity(capacity);
    let mut values = Vec::<A>::with_capacity(capacity);
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next()? {
        extend_from_page(
            page,
            metadata.descriptor(),
            &mut indices,
            &mut values,
            &mut validity,
            op,
        )?
    }

    let keys = PrimitiveArray::new(K::PRIMITIVE.into(), indices.into(), validity.into());
    let data_type = DictionaryArray::<K>::get_child(&data_type).clone();
    let values = Arc::new(PrimitiveArray::new(data_type, values.into(), None));
    Ok(Box::new(DictionaryArray::<K>::from_data(keys, values)))
}
