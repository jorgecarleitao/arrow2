use parquet2::{
    encoding::{delta_length_byte_array, hybrid_rle, Encoding},
    metadata::ColumnDescriptor,
    page::{BinaryPageDict, DataPage},
};

use crate::{
    array::Offset,
    bitmap::MutableBitmap,
    error::Result,
    io::parquet::read::utils::{extend_from_decoder, Pushable},
};

use super::{super::utils, utils::Binary};

#[inline]
fn values_iter<'a>(
    indices_buffer: &'a [u8],
    dict: &'a BinaryPageDict,
    additional: usize,
) -> impl Iterator<Item = &'a [u8]> + 'a {
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let indices = hybrid_rle::HybridRleDecoder::new(indices_buffer, bit_width as u32, additional);
    indices.map(move |index| {
        let index = index as usize;
        let dict_offset_i = dict_offsets[index] as usize;
        let dict_offset_ip1 = dict_offsets[index + 1] as usize;
        &dict_values[dict_offset_i..dict_offset_ip1]
    })
}

/// Assumptions: No rep levels
#[allow(clippy::too_many_arguments)]
fn read_dict_buffer<O: Offset>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    additional: usize,
    dict: &BinaryPageDict,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    let values_iterator = values_iter(indices_buffer, dict, additional);

    let mut validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    extend_from_decoder(
        validity,
        &mut validity_iterator,
        additional,
        values,
        values_iterator,
    );
}

#[allow(clippy::too_many_arguments)]
fn read_dict_required<O: Offset>(
    indices_buffer: &[u8],
    additional: usize,
    dict: &BinaryPageDict,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    debug_assert_eq!(0, validity.len());
    let values_iterator = values_iter(indices_buffer, dict, additional);
    for value in values_iterator {
        values.push(value);
    }
}

struct Offsets<'a, O: Offset>(pub &'a mut Vec<O>);

impl<'a, O: Offset> Pushable<O> for Offsets<'a, O> {
    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional)
    }

    #[inline]
    fn push(&mut self, value: O) {
        self.0.push(value)
    }

    #[inline]
    fn push_null(&mut self) {
        self.0.push(*self.0.last().unwrap())
    }

    #[inline]
    fn extend_constant(&mut self, additional: usize, value: O) {
        self.0.extend_constant(additional, value)
    }
}

fn read_delta_optional<O: Offset>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    let Binary {
        offsets,
        values,
        last_offset,
    } = values;

    // values_buffer: first 4 bytes are len, remaining is values
    let mut values_iterator = delta_length_byte_array::Decoder::new(values_buffer);
    let offsets_iterator = values_iterator.by_ref().map(|x| {
        *last_offset += O::from_usize(x as usize).unwrap();
        *last_offset
    });

    let mut validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    // offsets:
    extend_from_decoder(
        validity,
        &mut validity_iterator,
        additional,
        &mut Offsets::<O>(offsets),
        offsets_iterator,
    );

    // values:
    let new_values = values_iterator.into_values();
    values.extend_from_slice(new_values);
}

fn read_plain_optional<O: Offset>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    additional: usize,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) {
    // values_buffer: first 4 bytes are len, remaining is values
    let values_iterator = utils::BinaryIter::new(values_buffer);

    let mut validity_iterator = hybrid_rle::Decoder::new(validity_buffer, 1);

    extend_from_decoder(
        validity,
        &mut validity_iterator,
        additional,
        values,
        values_iterator,
    )
}

pub(super) fn read_plain_required<O: Offset>(
    buffer: &[u8],
    additional: usize,
    values: &mut Binary<O>,
) {
    let values_iterator = utils::BinaryIter::new(buffer);

    // each value occupies 4 bytes + len declared in 4 bytes => reserve accordingly.
    values.offsets.reserve(additional);
    values.values.reserve(buffer.len() - 4 * additional);
    let a = values.values.capacity();
    for value in values_iterator {
        values.push(value);
    }
    debug_assert_eq!(a, values.values.capacity());
}

pub(super) fn extend_from_page<O: Offset>(
    page: &DataPage,
    descriptor: &ColumnDescriptor,
    values: &mut Binary<O>,
    validity: &mut MutableBitmap,
) -> Result<()> {
    let additional = page.num_values();
    assert_eq!(descriptor.max_rep_level(), 0);
    assert!(descriptor.max_def_level() <= 1);
    let is_optional = descriptor.max_def_level() == 1;

    let (_, validity_buffer, values_buffer, version) = utils::split_buffer(page, descriptor);

    match (&page.encoding(), page.dictionary_page(), is_optional) {
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
            read_dict_buffer::<O>(
                validity_buffer,
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                values,
                validity,
            )
        }
        (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
            read_dict_required::<O>(
                values_buffer,
                additional,
                dict.as_any().downcast_ref().unwrap(),
                values,
                validity,
            )
        }
        (Encoding::DeltaLengthByteArray, None, true) => {
            read_delta_optional::<O>(validity_buffer, values_buffer, additional, values, validity)
        }
        (Encoding::Plain, _, true) => {
            read_plain_optional::<O>(validity_buffer, values_buffer, additional, values, validity)
        }
        (Encoding::Plain, _, false) => {
            read_plain_required::<O>(page.buffer(), page.num_values(), values)
        }
        _ => {
            return Err(utils::not_implemented(
                &page.encoding(),
                is_optional,
                page.dictionary_page().is_some(),
                version,
                "Binary",
            ))
        }
    };
    Ok(())
}
