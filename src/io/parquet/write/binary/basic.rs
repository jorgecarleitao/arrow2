use parquet2::{
    encoding::{delta_bitpacked, Encoding},
    metadata::ColumnDescriptor,
    page::CompressedDataPage,
    statistics::{serialize_statistics, BinaryStatistics, ParquetStatistics, Statistics},
    write::WriteOptions,
};

use super::super::utils;
use crate::{
    array::{Array, BinaryArray, Offset},
    bitmap::Bitmap,
    error::{ArrowError, Result},
    io::parquet::read::is_type_nullable,
};

pub(crate) fn encode_plain<O: Offset>(
    array: &BinaryArray<O>,
    is_optional: bool,
    buffer: &mut Vec<u8>,
) {
    // append the non-null values
    if is_optional {
        array.iter().for_each(|x| {
            if let Some(x) = x {
                // BYTE_ARRAY: first 4 bytes denote length in littleendian.
                let len = (x.len() as u32).to_le_bytes();
                buffer.extend_from_slice(&len);
                buffer.extend_from_slice(x);
            }
        })
    } else {
        array.values_iter().for_each(|x| {
            // BYTE_ARRAY: first 4 bytes denote length in littleendian.
            let len = (x.len() as u32).to_le_bytes();
            buffer.extend_from_slice(&len);
            buffer.extend_from_slice(x);
        })
    }
}

pub fn array_to_page<O: Offset>(
    array: &BinaryArray<O>,
    options: WriteOptions,
    descriptor: ColumnDescriptor,
    encoding: Encoding,
) -> Result<CompressedDataPage> {
    let validity = array.validity();
    let is_optional = is_type_nullable(descriptor.type_());

    let mut buffer = vec![];
    utils::write_def_levels(
        &mut buffer,
        is_optional,
        validity,
        array.len(),
        options.version,
    )?;

    let definition_levels_byte_length = buffer.len();

    match encoding {
        Encoding::Plain => encode_plain(array, is_optional, &mut buffer),
        Encoding::DeltaLengthByteArray => encode_delta(
            array.values(),
            array.offsets(),
            array.validity(),
            is_optional,
            &mut buffer,
        ),
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Datatype {:?} cannot be encoded by {:?} encoding",
                array.data_type(),
                encoding
            )))
        }
    }

    let uncompressed_page_size = buffer.len();

    let mut compressed_buffer = vec![];
    let _was_compressed = utils::compress(
        &mut buffer,
        &mut compressed_buffer,
        options,
        definition_levels_byte_length,
    )?;

    let statistics = if options.write_statistics {
        Some(build_statistics(array, descriptor.clone()))
    } else {
        None
    };

    utils::build_plain_page(
        compressed_buffer,
        array.len(),
        array.null_count(),
        uncompressed_page_size,
        0,
        definition_levels_byte_length,
        statistics,
        descriptor,
        options,
        encoding,
    )
}

pub(super) fn build_statistics<O: Offset>(
    array: &BinaryArray<O>,
    descriptor: ColumnDescriptor,
) -> ParquetStatistics {
    let statistics = &BinaryStatistics {
        descriptor,
        null_count: Some(array.null_count() as i64),
        distinct_count: None,
        max_value: array
            .iter()
            .flatten()
            .max_by(|x, y| ord_binary(x, y))
            .map(|x| x.to_vec()),
        min_value: array
            .iter()
            .flatten()
            .min_by(|x, y| ord_binary(x, y))
            .map(|x| x.to_vec()),
    } as &dyn Statistics;
    serialize_statistics(statistics)
}

pub(crate) fn encode_delta<O: Offset>(
    values: &[u8],
    offsets: &[O],
    validity: Option<&Bitmap>,
    is_optional: bool,
    buffer: &mut Vec<u8>,
) {
    if is_optional {
        if let Some(validity) = validity {
            let lengths = offsets
                .windows(2)
                .map(|w| (w[1] - w[0]).to_isize() as i32)
                .zip(validity.iter())
                .flat_map(|(x, is_valid)| if is_valid { Some(x) } else { None });
            let length = offsets.len() - 1 - validity.null_count();
            let lengths = utils::ExactSizedIter::new(lengths, length);

            delta_bitpacked::encode(lengths, buffer);
        } else {
            let lengths = offsets.windows(2).map(|w| (w[1] - w[0]).to_isize() as i32);
            delta_bitpacked::encode(lengths, buffer);
        }
    } else {
        let lengths = offsets.windows(2).map(|w| (w[1] - w[0]).to_isize() as i32);
        delta_bitpacked::encode(lengths, buffer);
    }

    buffer.extend_from_slice(
        &values[offsets.first().unwrap().to_usize()..offsets.last().unwrap().to_usize()],
    )
}

/// Returns the ordering of two binary values. This corresponds to pyarrows' ordering
/// of statistics.
pub(crate) fn ord_binary<'a>(a: &'a [u8], b: &'a [u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a.is_empty(), b.is_empty()) {
        (true, true) => return Equal,
        (true, false) => return Less,
        (false, true) => return Greater,
        (false, false) => {}
    }

    for (v1, v2) in a.iter().zip(b.iter()) {
        match v1.cmp(v2) {
            Equal => continue,
            other => return other,
        }
    }
    Equal
}
