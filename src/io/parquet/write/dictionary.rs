use parquet2::{
    encoding::{hybrid_rle::encode_u32, Encoding},
    metadata::ColumnDescriptor,
    page::{CompressedDictPage, CompressedPage},
    write::{DynIter, WriteOptions},
};

use super::binary::encode_plain as binary_encode_plain;
use super::primitive::encode_plain as primitive_encode_plain;
use super::utf8::encode_plain as utf8_encode_plain;
use crate::array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray};
use crate::bitmap::Bitmap;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::io::parquet::read::is_type_nullable;
use crate::io::parquet::write::utils;

fn encode_keys<K: DictionaryKey>(
    array: &PrimitiveArray<K>,
    // todo: merge this to not discard values' validity
    validity: Option<&Bitmap>,
    descriptor: ColumnDescriptor,
    options: WriteOptions,
) -> Result<CompressedPage> {
    let is_optional = is_type_nullable(descriptor.type_());

    let mut buffer = vec![];

    let null_count = if let Some(validity) = validity {
        let projected_validity = array.iter().map(|x| {
            x.map(|x| validity.get_bit(x.to_usize().unwrap()))
                .unwrap_or(false)
        });
        let projected_val = Bitmap::from_trusted_len_iter(projected_validity);

        let null_count = projected_val.null_count();

        utils::write_def_levels(
            &mut buffer,
            is_optional,
            Some(&projected_val),
            array.len(),
            options.version,
        )?;
        null_count
    } else {
        utils::write_def_levels(
            &mut buffer,
            is_optional,
            array.validity(),
            array.len(),
            options.version,
        )?;
        array.null_count()
    };

    let definition_levels_byte_length = buffer.len();

    // encode indices
    // compute the required number of bits
    if let Some(validity) = validity {
        let keys = array
            .iter()
            .flatten()
            .map(|x| {
                let index = x.to_usize().unwrap();
                // discard indices whose values are null, since they are part of the def levels.
                if validity.get_bit(index) {
                    Some(index as u32)
                } else {
                    None
                }
            })
            .flatten();
        let num_bits = utils::get_bit_width(keys.clone().max().unwrap_or(0) as u64) as u8;

        let keys = utils::ExactSizedIter::new(keys, array.len() - null_count);

        // num_bits as a single byte
        buffer.push(num_bits);

        // followed by the encoded indices.
        encode_u32(&mut buffer, keys, num_bits)?;
    } else {
        let keys = array.iter().flatten().map(|x| x.to_usize().unwrap() as u32);
        let num_bits = utils::get_bit_width(keys.clone().max().unwrap_or(0) as u64) as u8;

        let keys = utils::ExactSizedIter::new(keys, array.len() - array.null_count());

        // num_bits as a single byte
        buffer.push(num_bits);

        // followed by the encoded indices.
        encode_u32(&mut buffer, keys, num_bits)?;
    }

    let uncompressed_page_size = buffer.len();

    let buffer = utils::compress(buffer, options, definition_levels_byte_length)?;

    utils::build_plain_page(
        buffer,
        array.len(),
        array.null_count(),
        uncompressed_page_size,
        0,
        definition_levels_byte_length,
        None,
        descriptor,
        options,
        Encoding::RleDictionary,
    )
    .map(CompressedPage::Data)
}

macro_rules! dyn_prim {
    ($from:ty, $to:ty, $array:expr, $options:expr) => {{
        let values = $array.values().as_any().downcast_ref().unwrap();

        let mut buffer = vec![];
        primitive_encode_plain::<$from, $to>(values, false, &mut buffer);
        let buffer = utils::compress(buffer, $options, 0)?;

        CompressedPage::Dict(CompressedDictPage::new(buffer, values.len()))
    }};
}

pub fn array_to_pages<K: DictionaryKey>(
    array: &DictionaryArray<K>,
    descriptor: ColumnDescriptor,
    options: WriteOptions,
    encoding: Encoding,
) -> Result<DynIter<'static, Result<CompressedPage>>>
where
    PrimitiveArray<K>: std::fmt::Display,
{
    match encoding {
        Encoding::PlainDictionary | Encoding::RleDictionary => {
            // write DictPage
            let dict_page = match array.values().data_type().to_logical_type() {
                DataType::Int8 => dyn_prim!(i8, i32, array, options),
                DataType::Int16 => dyn_prim!(i16, i32, array, options),
                DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
                    dyn_prim!(i32, i32, array, options)
                }
                DataType::Int64
                | DataType::Date64
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Duration(_) => dyn_prim!(i64, i64, array, options),
                DataType::UInt8 => dyn_prim!(u8, i32, array, options),
                DataType::UInt16 => dyn_prim!(u16, i32, array, options),
                DataType::UInt32 => dyn_prim!(u32, i32, array, options),
                DataType::UInt64 => dyn_prim!(i64, i64, array, options),
                DataType::Utf8 => {
                    let values = array.values().as_any().downcast_ref().unwrap();

                    let mut buffer = vec![];
                    utf8_encode_plain::<i32>(values, false, &mut buffer);
                    let buffer = utils::compress(buffer, options, 0)?;
                    CompressedPage::Dict(CompressedDictPage::new(buffer, values.len()))
                }
                DataType::LargeUtf8 => {
                    let values = array.values().as_any().downcast_ref().unwrap();

                    let mut buffer = vec![];
                    utf8_encode_plain::<i64>(values, false, &mut buffer);
                    let buffer = utils::compress(buffer, options, 0)?;
                    CompressedPage::Dict(CompressedDictPage::new(buffer, values.len()))
                }
                DataType::Binary => {
                    let values = array.values().as_any().downcast_ref().unwrap();

                    let mut buffer = vec![];
                    binary_encode_plain::<i32>(values, false, &mut buffer);
                    let buffer = utils::compress(buffer, options, 0)?;
                    CompressedPage::Dict(CompressedDictPage::new(buffer, values.len()))
                }
                DataType::LargeBinary => {
                    let values = array.values().as_any().downcast_ref().unwrap();

                    let mut buffer = vec![];
                    binary_encode_plain::<i64>(values, false, &mut buffer);
                    let buffer = utils::compress(buffer, options, 0)?;
                    CompressedPage::Dict(CompressedDictPage::new(buffer, values.len()))
                }
                other => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Writing dictionary arrays to parquet only support data type {:?}",
                        other
                    )))
                }
            };

            // write DataPage pointing to DictPage
            let data_page =
                encode_keys(array.keys(), array.values().validity(), descriptor, options)?;

            let iter = std::iter::once(Ok(dict_page)).chain(std::iter::once(Ok(data_page)));
            Ok(DynIter::new(Box::new(iter)))
        }
        _ => Err(ArrowError::NotYetImplemented(
            "Dictionary arrays only support dictionary encoding".to_string(),
        )),
    }
}
