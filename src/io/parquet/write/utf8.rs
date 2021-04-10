use parquet2::{
    compression::create_codec,
    encoding::{hybrid_rle::encode, Encoding},
    read::{CompressedPage, PageV1},
    schema::{CompressionCodec, DataPageHeader},
};

use crate::{
    array::{Array, Offset, Utf8Array},
    error::Result,
};

pub fn array_to_page_v1<O: Offset>(
    array: &Utf8Array<O>,
    compression: CompressionCodec,
) -> Result<CompressedPage> {
    let validity = array.validity();

    // parquet: first 4 bytes represent the length in bytes
    let mut buffer = std::io::Cursor::new(vec![0; 4]);
    buffer.set_position(4);

    // encode def levels
    if let Some(validity) = validity {
        encode(&mut buffer, validity.iter())?;
    }
    let mut buffer = buffer.into_inner();
    let length = buffer.len() - 4;
    // todo: pay this small debt (loop?)
    let length = length.to_le_bytes();
    buffer[0] = length[0];
    buffer[1] = length[1];
    buffer[2] = length[2];
    buffer[3] = length[3];

    // append the non-null values
    array.iter().for_each(|x| {
        if let Some(x) = x {
            // BYTE_ARRAY: first 4 bytes denote length in littleendian.
            let len = (x.len() as u32).to_le_bytes();
            buffer.extend_from_slice(&len);
            buffer.extend_from_slice(x.as_bytes());
        }
    });
    let uncompressed_page_size = buffer.len();
    println!("{:?}", buffer);

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

    let header = DataPageHeader {
        num_values: array.len() as i32,
        encoding: Encoding::Plain,
        definition_level_encoding: Encoding::Rle,
        repetition_level_encoding: Encoding::Rle,
        statistics: None,
    };

    Ok(CompressedPage::V1(PageV1 {
        buffer,
        header,
        compression,
        uncompressed_page_size,
        dictionary_page: None,
    }))
}
