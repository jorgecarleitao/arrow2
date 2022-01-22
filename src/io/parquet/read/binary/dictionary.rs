use std::sync::Arc;

use parquet2::{
    encoding::{hybrid_rle, Encoding},
    metadata::{ColumnChunkMetaData, ColumnDescriptor},
    page::{BinaryPageDict, DataPage},
    FallibleStreamingIterator,
};

use super::{super::utils as other_utils, utils::Binary};
use crate::{
    array::{
        Array, BinaryArray, DictionaryArray, DictionaryKey, Offset, PrimitiveArray, Utf8Array,
    },
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
    io::parquet::read::utils::extend_from_decoder,
};

#[allow(clippy::too_many_arguments)]
fn read_dict_optional<K, O>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &BinaryPageDict,
    indices: &mut Vec<K>,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) where
    K: DictionaryKey,
    O: Offset,
{
    let length = indices.len() + additional;
    values.values.extend_from_slice(dict.values());
    values.offsets.extend(
        dict.offsets()
            .iter()
            .map(|x| O::from_usize(*x as usize).unwrap()),
    );

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
        length,
        indices,
        indices_iter,
    )
}

fn extend_from_page<K, O>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    indices: &mut Vec<K>,
    values: &mut Binary<O>,
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
    I: FallibleStreamingIterator<Item = DataPage, Error = E>,
{
    let capacity = metadata.num_values() as usize;
    let mut indices = Vec::<K>::with_capacity(capacity);
    let mut values = Binary::<O>::with_capacity(capacity);
    values.offsets.clear();
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next()? {
        extend_from_page(
            page,
            metadata.descriptor(),
            &mut indices,
            &mut values,
            &mut validity,
        )?
    }

    if values.offsets.is_empty() {
        // the array is empty and thus we need to push the first offset ourselves.
        values.offsets.push(O::zero());
    };
    let keys = PrimitiveArray::from_data(K::PRIMITIVE.into(), indices.into(), validity.into());
    let data_type = DictionaryArray::<K>::get_child(&data_type).clone();
    use crate::datatypes::PhysicalType;
    let values = match data_type.to_physical_type() {
        PhysicalType::Binary | PhysicalType::LargeBinary => Arc::new(BinaryArray::from_data(
            data_type,
            values.offsets.into(),
            values.values.into(),
            None,
        )) as Arc<dyn Array>,
        PhysicalType::Utf8 | PhysicalType::LargeUtf8 => Arc::new(Utf8Array::from_data(
            data_type,
            values.offsets.into(),
            values.values.into(),
            None,
        )),
        _ => unreachable!(),
    };
    Ok(Box::new(DictionaryArray::<K>::from_data(keys, values)))
}
