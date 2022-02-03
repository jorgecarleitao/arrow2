use crate::array::Offset;
use crate::bitmap::MutableBitmap;

/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
#[allow(clippy::type_complexity)]
pub(crate) unsafe fn try_trusted_len_unzip<E, I, P, O>(
    iterator: I,
) -> std::result::Result<(Option<MutableBitmap>, Vec<O>, Vec<u8>), E>
where
    O: Offset,
    P: AsRef<[u8]>,
    I: Iterator<Item = std::result::Result<Option<P>, E>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut null = MutableBitmap::with_capacity(len);
    let mut offsets = Vec::<O>::with_capacity(len + 1);
    let mut values = Vec::<u8>::new();

    let mut length = O::default();
    let mut dst = offsets.as_mut_ptr();
    std::ptr::write(dst, length);
    dst = dst.add(1);
    for item in iterator {
        if let Some(item) = item? {
            null.push_unchecked(true);
            let s = item.as_ref();
            length += O::from_usize(s.len()).unwrap();
            values.extend_from_slice(s);
        } else {
            null.push_unchecked(false);
        };

        std::ptr::write(dst, length);
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(offsets.as_ptr()) as usize,
        len + 1,
        "Trusted iterator length was not accurately reported"
    );
    offsets.set_len(len + 1);

    Ok((null.into(), offsets, values))
}

/// Creates [`MutableBitmap`] and two [`Vec`]s from an iterator of `Option`.
/// The first buffer corresponds to a offset buffer, the second one
/// corresponds to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn trusted_len_unzip<O, I, P>(
    iterator: I,
) -> (Option<MutableBitmap>, Vec<O>, Vec<u8>)
where
    O: Offset,
    P: AsRef<[u8]>,
    I: Iterator<Item = Option<P>>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut offsets = Vec::<O>::with_capacity(len + 1);
    let mut values = Vec::<u8>::new();
    let mut validity = MutableBitmap::new();

    offsets.push(O::default());

    extend_from_trusted_len_iter(&mut offsets, &mut values, &mut validity, iterator);

    let validity = if validity.null_count() > 0 {
        Some(validity)
    } else {
        None
    };

    (validity, offsets, values)
}

/// Creates two [`Buffer`]s from an iterator of `&[u8]`.
/// The first buffer corresponds to a offset buffer, the second to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is [`TrustedLen`].
#[inline]
pub(crate) unsafe fn trusted_len_values_iter<O, I, P>(iterator: I) -> (Vec<O>, Vec<u8>)
where
    O: Offset,
    P: AsRef<[u8]>,
    I: Iterator<Item = P>,
{
    let (_, upper) = iterator.size_hint();
    let len = upper.expect("trusted_len_unzip requires an upper limit");

    let mut offsets = Vec::<O>::with_capacity(len + 1);
    let mut values = Vec::<u8>::new();

    offsets.push(O::default());

    extend_from_trusted_len_values_iter(&mut offsets, &mut values, iterator);

    (offsets, values)
}

// Populates `offsets` and `values` [`Vec`]s with information extracted
// from the incoming `iterator`.
// # Safety
// The caller must ensure the `iterator` is [`TrustedLen`]
#[inline]
pub(crate) unsafe fn extend_from_trusted_len_values_iter<I, P, O>(
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
    iterator: I,
) where
    O: Offset,
    P: AsRef<[u8]>,
    I: Iterator<Item = P>,
{
    let (_, upper) = iterator.size_hint();
    let additional = upper.expect("extend_from_trusted_len_values_iter requires an upper limit");

    offsets.reserve(additional);

    // Read in the last offset, will be used to increment and store
    // new values later on
    let mut length = *offsets.last().unwrap();

    // Get a mutable pointer to the `offsets`, and move the pointer
    // to the position, where a new value will be written
    let mut dst = offsets.as_mut_ptr();
    dst = dst.add(offsets.len());

    for item in iterator {
        let s = item.as_ref();

        // Calculate the new offset value
        length += O::from_usize(s.len()).unwrap();

        // Push new entries for both `values` and `offsets` buffer
        values.extend_from_slice(s);
        std::ptr::write(dst, length);

        // Move to the next position in offset buffer
        dst = dst.add(1);
    }

    debug_assert_eq!(
        dst.offset_from(offsets.as_ptr()) as usize,
        offsets.len() + additional,
        "TrustedLen iterator's length was not accurately reported"
    );

    // We make sure to set the new length for the `offsets` buffer
    offsets.set_len(offsets.len() + additional);
}

// Populates `offsets` and `values` [`Vec`]s with information extracted
// from the incoming `iterator`.

// the return value indicates how many items were added.
#[inline]
pub(crate) fn extend_from_values_iter<I, P, O>(
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
    iterator: I,
) -> usize
where
    O: Offset,
    P: AsRef<[u8]>,
    I: Iterator<Item = P>,
{
    let (size_hint, _) = iterator.size_hint();

    offsets.reserve(size_hint);

    // Read in the last offset, will be used to increment and store
    // new values later on
    let mut length = *offsets.last().unwrap();
    let start_index = offsets.len();

    for item in iterator {
        let s = item.as_ref();
        // Calculate the new offset value
        length += O::from_usize(s.len()).unwrap();

        values.extend_from_slice(s);
        offsets.push(length);
    }
    offsets.len() - start_index
}

// Populates `offsets`, `values`, and `validity` [`Vec`]s with
// information extracted from the incoming `iterator`.
//
// # Safety
// The caller must ensure that `iterator` is [`TrustedLen`]
#[inline]
pub(crate) unsafe fn extend_from_trusted_len_iter<O, I, P>(
    offsets: &mut Vec<O>,
    values: &mut Vec<u8>,
    validity: &mut MutableBitmap,
    iterator: I,
) where
    O: Offset,
    P: AsRef<[u8]>,
    I: Iterator<Item = Option<P>>,
{
    let (_, upper) = iterator.size_hint();
    let additional = upper.expect("extend_from_trusted_len_iter requires an upper limit");

    offsets.reserve(additional);
    validity.reserve(additional);

    // Read in the last offset, will be used to increment and store
    // new values later on
    let mut length = *offsets.last().unwrap();

    // Get a mutable pointer to the `offsets`, and move the pointer
    // to the position, where a new value will be written
    let mut dst = offsets.as_mut_ptr();
    dst = dst.add(offsets.len());

    for item in iterator {
        if let Some(item) = item {
            let bytes = item.as_ref();

            // Calculate new offset value
            length += O::from_usize(bytes.len()).unwrap();

            // Push new values for `values` and `validity` buffer
            values.extend_from_slice(bytes);
            validity.push_unchecked(true);
        } else {
            // If `None`, update only `validity`
            validity.push_unchecked(false);
        }

        // Push new offset or old offset depending on the `item`
        std::ptr::write(dst, length);

        // Move to the next position in offset buffer
        dst = dst.add(1);
    }

    debug_assert_eq!(
        dst.offset_from(offsets.as_ptr()) as usize,
        offsets.len() + additional,
        "TrustedLen iterator's length was not accurately reported"
    );

    // We make sure to set the new length for the `offsets` buffer
    offsets.set_len(offsets.len() + additional);
}

/// Creates two [`Vec`]s from an iterator of `&[u8]`.
/// The first buffer corresponds to a offset buffer, the second to a values buffer.
#[inline]
pub(crate) fn values_iter<O, I, P>(iterator: I) -> (Vec<O>, Vec<u8>)
where
    O: Offset,
    P: AsRef<[u8]>,
    I: Iterator<Item = P>,
{
    let (lower, _) = iterator.size_hint();

    let mut offsets = Vec::<O>::with_capacity(lower + 1);
    let mut values = Vec::<u8>::new();

    let mut length = O::default();
    offsets.push(length);

    for item in iterator {
        let s = item.as_ref();
        length += O::from_usize(s.len()).unwrap();
        values.extend_from_slice(s);

        offsets.push(length)
    }
    (offsets, values)
}
