use std::sync::Arc;

use parquet2::{
    encoding::Encoding,
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
    io::parquet::read::utils::read_dict_optional,
};

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
            let dict = dict.as_any().downcast_ref::<BinaryPageDict>().unwrap();

            values.values.extend_from_slice(dict.values());
            values.offsets.0.extend(
                dict.offsets()
                    .iter()
                    .map(|x| O::from_usize(*x as usize).unwrap()),
            );

            read_dict_optional(
                validity_buffer,
                values_buffer,
                additional,
                indices,
                validity,
            )
        }
        _ => {
            return Err(other_utils::not_implemented(
                &page.encoding(),
                is_optional,
                page.dictionary_page().is_some(),
                version,
                "binary",
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
    values.offsets.0.clear();
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

    if values.offsets.0.is_empty() {
        // the array is empty and thus we need to push the first offset ourselves.
        values.offsets.0.push(O::zero());
    };
    let keys = PrimitiveArray::from_data(K::PRIMITIVE.into(), indices.into(), validity.into());
    let data_type = DictionaryArray::<K>::get_child(&data_type).clone();
    use crate::datatypes::PhysicalType;
    let values = match data_type.to_physical_type() {
        PhysicalType::Binary | PhysicalType::LargeBinary => Arc::new(BinaryArray::from_data(
            data_type,
            values.offsets.0.into(),
            values.values.into(),
            None,
        )) as Arc<dyn Array>,
        PhysicalType::Utf8 | PhysicalType::LargeUtf8 => Arc::new(Utf8Array::from_data(
            data_type,
            values.offsets.0.into(),
            values.values.into(),
            None,
        )),
        _ => unreachable!(),
    };
    Ok(Box::new(DictionaryArray::<K>::from_data(keys, values)))
}
