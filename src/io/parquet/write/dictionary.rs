use parquet2::{
    encoding::{hybrid_rle::encode_u32, Encoding},
    metadata::Descriptor,
    page::{EncodedDictPage, EncodedPage},
    statistics::{serialize_statistics, ParquetStatistics},
    write::DynIter,
};

use super::binary::build_statistics as binary_build_statistics;
use super::binary::encode_plain as binary_encode_plain;
use super::fixed_len_bytes::build_statistics as fixed_binary_build_statistics;
use super::fixed_len_bytes::encode_plain as fixed_binary_encode_plain;
use super::primitive::build_statistics as primitive_build_statistics;
use super::primitive::encode_plain as primitive_encode_plain;
use super::utf8::build_statistics as utf8_build_statistics;
use super::utf8::encode_plain as utf8_encode_plain;
use super::WriteOptions;
use crate::bitmap::Bitmap;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::io::parquet::write::utils;
use crate::{
    array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray},
    io::parquet::read::schema::is_nullable,
};

fn encode_keys<K: DictionaryKey>(
    array: &PrimitiveArray<K>,
    validity: Option<&Bitmap>,
    descriptor: Descriptor,
    statistics: ParquetStatistics,
    options: WriteOptions,
) -> Result<EncodedPage> {
    let is_optional = is_nullable(&descriptor.primitive_type.field_info);

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
        let keys = array.iter().flatten().filter_map(|x| {
            let index = x.to_usize().unwrap();
            // discard indices whose values are null, since they are part of the def levels.
            if validity.get_bit(index) {
                Some(index as u32)
            } else {
                None
            }
        });
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

    utils::build_plain_page(
        buffer,
        array.len(),
        array.len(),
        array.null_count(),
        0,
        definition_levels_byte_length,
        Some(statistics),
        descriptor,
        options,
        Encoding::RleDictionary,
    )
    .map(EncodedPage::Data)
}

macro_rules! dyn_prim {
    ($from:ty, $to:ty, $array:expr, $options:expr, $descriptor:expr) => {{
        let values = $array.values().as_any().downcast_ref().unwrap();

        let mut buffer = vec![];
        primitive_encode_plain::<$from, $to>(values, false, &mut buffer);
        let stats =
            primitive_build_statistics::<$from, $to>(values, $descriptor.primitive_type.clone());
        let stats = serialize_statistics(&stats);
        (EncodedDictPage::new(buffer, values.len()), stats)
    }};
}

pub fn array_to_pages<K: DictionaryKey>(
    array: &DictionaryArray<K>,
    descriptor: Descriptor,
    options: WriteOptions,
    encoding: Encoding,
) -> Result<DynIter<'static, Result<EncodedPage>>> {
    match encoding {
        Encoding::PlainDictionary | Encoding::RleDictionary => {
            // write DictPage
            let (dict_page, statistics) = match array.values().data_type().to_logical_type() {
                DataType::Int8 => dyn_prim!(i8, i32, array, options, descriptor),
                DataType::Int16 => dyn_prim!(i16, i32, array, options, descriptor),
                DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
                    dyn_prim!(i32, i32, array, options, descriptor)
                }
                DataType::Int64
                | DataType::Date64
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Duration(_) => dyn_prim!(i64, i64, array, options, descriptor),
                DataType::UInt8 => dyn_prim!(u8, i32, array, options, descriptor),
                DataType::UInt16 => dyn_prim!(u16, i32, array, options, descriptor),
                DataType::UInt32 => dyn_prim!(u32, i32, array, options, descriptor),
                DataType::UInt64 => dyn_prim!(i64, i64, array, options, descriptor),
                DataType::Float32 => dyn_prim!(f32, f32, array, options, descriptor),
                DataType::Float64 => dyn_prim!(f64, f64, array, options, descriptor),
                DataType::Utf8 => {
                    let array = array.values().as_any().downcast_ref().unwrap();

                    let mut buffer = vec![];
                    utf8_encode_plain::<i32>(array, false, &mut buffer);
                    let stats = utf8_build_statistics(array, descriptor.primitive_type.clone());
                    (EncodedDictPage::new(buffer, array.len()), stats)
                }
                DataType::LargeUtf8 => {
                    let array = array.values().as_any().downcast_ref().unwrap();

                    let mut buffer = vec![];
                    utf8_encode_plain::<i64>(array, false, &mut buffer);
                    let stats = utf8_build_statistics(array, descriptor.primitive_type.clone());
                    (EncodedDictPage::new(buffer, array.len()), stats)
                }
                DataType::Binary => {
                    let array = array.values().as_any().downcast_ref().unwrap();

                    let mut buffer = vec![];
                    binary_encode_plain::<i32>(array, false, &mut buffer);
                    let stats = binary_build_statistics(array, descriptor.primitive_type.clone());
                    (EncodedDictPage::new(buffer, array.len()), stats)
                }
                DataType::LargeBinary => {
                    let array = array.values().as_any().downcast_ref().unwrap();

                    let mut buffer = vec![];
                    binary_encode_plain::<i64>(array, false, &mut buffer);
                    let stats = binary_build_statistics(array, descriptor.primitive_type.clone());
                    (EncodedDictPage::new(buffer, array.len()), stats)
                }
                DataType::FixedSizeBinary(_) => {
                    let mut buffer = vec![];
                    let array = array.values().as_any().downcast_ref().unwrap();
                    fixed_binary_encode_plain(array, false, &mut buffer);
                    let stats =
                        fixed_binary_build_statistics(array, descriptor.primitive_type.clone());
                    let stats = serialize_statistics(&stats);
                    (EncodedDictPage::new(buffer, array.len()), stats)
                }
                other => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Writing dictionary arrays to parquet only support data type {:?}",
                        other
                    )))
                }
            };
            let dict_page = EncodedPage::Dict(dict_page);

            // write DataPage pointing to DictPage
            let data_page = encode_keys(
                array.keys(),
                array.values().validity(),
                descriptor,
                statistics,
                options,
            )?;

            let iter = std::iter::once(Ok(dict_page)).chain(std::iter::once(Ok(data_page)));
            Ok(DynIter::new(Box::new(iter)))
        }
        _ => Err(ArrowError::NotYetImplemented(
            "Dictionary arrays only support dictionary encoding".to_string(),
        )),
    }
}
