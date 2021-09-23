use crate::{
    array::{Array, Offset},
    bitmap::{Bitmap, MutableBitmap},
    buffer::MutableBuffer,
};

pub(super) fn extend_offsets<T: Offset>(
    buffer: &mut MutableBuffer<T>,
    last_offset: &mut T,
    offsets: &[T],
) {
    buffer.reserve(offsets.len() - 1);
    offsets.windows(2).for_each(|offsets| {
        // compute the new offset
        let length = offsets[1] - offsets[0];
        *last_offset += length;
        buffer.push(*last_offset);
    });
}

pub(super) type ExtendNullBits<'a> = Box<dyn Fn(&mut MutableBitmap, usize, usize) + 'a>;

pub(super) fn build_extend_null_bits(array: &dyn Array, use_validity: bool) -> ExtendNullBits {
    if let Some(bitmap) = array.validity() {
        Box::new(move |validity, start, len| {
            assert!(start + len <= bitmap.len());
            unsafe {
                let iter = (start..start + len).map(|i| bitmap.get_bit_unchecked(i));
                validity.extend_from_trusted_len_iter_unchecked(iter);
            };
        })
    } else if use_validity {
        Box::new(|validity, _, len| {
            let iter = (0..len).map(|_| true);
            unsafe {
                validity.extend_from_trusted_len_iter_unchecked(iter);
            };
        })
    } else {
        Box::new(|_, _, _| {})
    }
}

#[inline]
pub(super) fn extend_validity(
    mutable_validity: &mut MutableBitmap,
    validity: Option<&Bitmap>,
    start: usize,
    len: usize,
    use_validity: bool,
) {
    if let Some(bitmap) = validity {
        assert!(start + len <= bitmap.len());
        unsafe {
            let iter = (start..start + len).map(|i| bitmap.get_bit_unchecked(i));
            mutable_validity.extend_from_trusted_len_iter_unchecked(iter);
        };
    } else if use_validity {
        mutable_validity.extend_constant(len, true);
    };
}

#[inline]
pub(super) fn extend_offset_values<O: Offset>(
    buffer: &mut MutableBuffer<u8>,
    offsets: &[O],
    values: &[u8],
    start: usize,
    len: usize,
) {
    let start_values = offsets[start].to_usize();
    let end_values = offsets[start + len].to_usize();
    let new_values = &values[start_values..end_values];
    buffer.extend_from_slice(new_values);
}
