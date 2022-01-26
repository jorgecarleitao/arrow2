use parquet2::{
    encoding::{hybrid_rle, Encoding},
    page::{DataPage, PrimitivePageDict},
    types::NativeType,
};

use super::super::utils as other_utils;
use super::utils::chunks;
use super::ColumnDescriptor;
use crate::{
    bitmap::MutableBitmap, error::Result, io::parquet::read::utils::extend_from_decoder,
    types::NativeType as ArrowNativeType,
};

#[inline]
fn values_iter<'a, T, A, F>(
    indices_buffer: &'a [u8],
    dict_values: &'a [T],
    additional: usize,
    op: F,
) -> impl Iterator<Item = A> + 'a
where
    T: NativeType,
    A: ArrowNativeType,
    F: 'a + Fn(T) -> A,
{
    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);
    indices.map(move |index| op(dict_values[index as usize]))
}

fn read_dict_buffer_optional<T, A, F>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &PrimitivePageDict<T>,
    values: &mut Vec<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let values_iterator = values_iter(indices_buffer, dict.values(), additional, op);

    let mut validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    extend_from_decoder(
        validity,
        &mut validity_iterator,
        additional,
        values,
        values_iterator,
    );
}

fn read_dict_buffer_required<T, A, F>(
    indices_buffer: &[u8],
    additional: usize,
    dict: &PrimitivePageDict<T>,
    values: &mut Vec<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    debug_assert_eq!(0, validity.len());
    let values_iterator = values_iter(indices_buffer, dict.values(), additional, op);
    values.extend(values_iterator);
}

fn read_nullable<T, A, F>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    values: &mut Vec<A>,
    validity: &mut MutableBitmap,
    op: F,
) where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    let values_iterator = chunks(values_buffer).map(op);

    let mut validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    extend_from_decoder(
        validity,
        &mut validity_iterator,
        additional,
        values,
        values_iterator,
    )
}

fn read_required<T, A, F>(values_buffer: &[u8], additional: usize, values: &mut Vec<A>, op: F)
where
    T: NativeType,
    A: ArrowNativeType,
    F: Fn(T) -> A,
{
    assert_eq!(values_buffer.len(), additional * std::mem::size_of::<T>());
    let iterator = chunks(values_buffer);

    let iterator = iterator.map(op);

    values.extend(iterator);
}

pub fn extend_from_page<T, A, F>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    values: &mut Vec<A>,
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

    let (_, validity_buffer, values_buffer, version) = other_utils::split_buffer(page, descriptor);

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
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
            read_dict_buffer_required(
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                values,
                validity,
                op,
            )
        }
        // it can happen that there is a dictionary but the encoding is plain because
        // it falled back.
        (Encoding::Plain, _, true) => read_nullable(
            validity_buffer,
            values_buffer,
            additional,
            values,
            validity,
            op,
        ),
        (Encoding::Plain, _, false) => read_required(page.buffer(), additional, values, op),
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
