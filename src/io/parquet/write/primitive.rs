use parquet2::{
    compression::create_codec,
    encoding::Encoding,
    read::{CompressedPage, PageHeader},
    schema::{CompressionCodec, DataPageHeader},
    types::NativeType,
};

use super::utils;
use crate::{
    array::{Array, PrimitiveArray},
    error::Result,
    types::NativeType as ArrowNativeType,
};

pub fn array_to_page_v1<T, R>(
    array: &PrimitiveArray<T>,
    compression: CompressionCodec,
    is_optional: bool,
) -> Result<CompressedPage>
where
    T: ArrowNativeType,
    R: NativeType,
    T: num::cast::AsPrimitive<R>,
{
    let validity = array.validity();

    let mut buffer = utils::write_def_levels(is_optional, validity, array.len())?;

    if is_optional {
        // append the non-null values
        array.iter().for_each(|x| {
            if let Some(x) = x {
                let parquet_native: R = x.as_();
                buffer.extend_from_slice(parquet_native.to_le_bytes().as_ref())
            }
        });
    } else {
        // append all values
        array.values().iter().for_each(|x| {
            let parquet_native: R = x.as_();
            buffer.extend_from_slice(parquet_native.to_le_bytes().as_ref())
        });
    }
    let uncompressed_page_size = buffer.len();

    let codec = create_codec(&compression)?;
    let buffer = if let Some(mut codec) = codec {
        // todo: remove this allocation by extending `buffer` directly.
        // needs refactoring `compress`'s API.
        let mut tmp = vec![];
        codec.compress(&buffer, &mut tmp)?;
        tmp
    } else {
        buffer
    };

    let header = PageHeader::V1(DataPageHeader {
        num_values: array.len() as i32,
        encoding: Encoding::Plain,
        definition_level_encoding: Encoding::Rle,
        repetition_level_encoding: Encoding::Rle,
        statistics: None,
    });

    Ok(CompressedPage::new(
        header,
        buffer,
        compression,
        uncompressed_page_size,
        None,
        None,
    ))
}
