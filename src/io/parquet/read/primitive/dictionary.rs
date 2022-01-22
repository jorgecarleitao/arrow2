use std::sync::Arc;

use parquet2::{
    encoding::Encoding,
    page::{DataPage, PrimitivePageDict},
    types::NativeType,
    FallibleStreamingIterator,
};

use super::super::utils;
use super::{ColumnChunkMetaData, ColumnDescriptor};
use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
    io::parquet::read::utils::read_dict_optional,
    types::NativeType as ArrowNativeType,
};

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
            let dict = dict
                .as_any()
                .downcast_ref::<PrimitivePageDict<T>>()
                .unwrap();
            values.extend(dict.values().iter().map(|x| op(*x)));
            read_dict_optional(
                validity_buffer,
                values_buffer,
                additional,
                indices,
                validity,
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

    let keys = PrimitiveArray::from_data(K::PRIMITIVE.into(), indices.into(), validity.into());
    let data_type = DictionaryArray::<K>::get_child(&data_type).clone();
    let values = Arc::new(PrimitiveArray::from_data(data_type, values.into(), None));
    Ok(Box::new(DictionaryArray::<K>::from_data(keys, values)))
}
