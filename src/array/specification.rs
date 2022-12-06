use crate::error::{Error, Result};
use crate::offset::{Offset, Offsets, OffsetsBuffer};

/// Helper trait to support `Offset` and `OffsetBuffer`
pub(crate) trait OffsetsContainer<O> {
    fn last(&self) -> usize;
    fn starts(&self) -> &[O];
}

impl<O: Offset> OffsetsContainer<O> for OffsetsBuffer<O> {
    #[inline]
    fn last(&self) -> usize {
        self.last().to_usize()
    }

    #[inline]
    fn starts(&self) -> &[O] {
        let last = self.buffer().len() - 1;
        unsafe { self.buffer().get_unchecked(0..last) }
    }
}

impl<O: Offset> OffsetsContainer<O> for Offsets<O> {
    #[inline]
    fn last(&self) -> usize {
        self.last().to_usize()
    }

    #[inline]
    fn starts(&self) -> &[O] {
        let offsets = self.as_slice();
        let last = offsets.len() - 1;
        unsafe { offsets.get_unchecked(0..last) }
    }
}

pub(crate) fn try_check_offsets_bounds<O: Offset, C: OffsetsContainer<O>>(
    offsets: &C,
    values_len: usize,
) -> Result<()> {
    if offsets.last() > values_len {
        Err(Error::oos("offsets must not exceed the values length"))
    } else {
        Ok(())
    }
}

/// # Error
/// * any offset is larger or equal to `values_len`.
/// * any slice of `values` between two consecutive pairs from `offsets` is invalid `utf8`, or
pub(crate) fn try_check_utf8<O: Offset, C: OffsetsContainer<O>>(
    offsets: &C,
    values: &[u8],
) -> Result<()> {
    try_check_offsets_bounds(offsets, values.len())?;

    if values.is_ascii() {
        Ok(())
    } else {
        simdutf8::basic::from_utf8(values)?;

        for start in offsets.starts() {
            let start = start.to_usize();

            // Safety: `try_check_offsets_bounds` just checked for bounds
            let b = *unsafe { values.get_unchecked(start) };

            // A valid code-point iff it does not start with 0b10xxxxxx
            // Bit-magic taken from `std::str::is_char_boundary`
            if (b as i8) < -0x40 {
                return Err(Error::oos("Non-valid char boundary detected"));
            }
        }
        Ok(())
    }
}

pub fn check_indexes<K>(keys: &[K], len: usize) -> Result<()>
where
    K: std::fmt::Debug + Copy + TryInto<usize>,
{
    keys.iter().try_for_each(|key| {
        let key: usize = (*key)
            .try_into()
            .map_err(|_| Error::oos(format!("The dictionary key must fit in a `usize`, but {:?} does not", key)))?;
        if key >= len {
            Err(Error::oos(format!("One of the dictionary keys is {} but it must be < than the length of the dictionary values, which is {}", key, len)))
        } else {
            Ok(())
        }
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    pub(crate) fn binary_strategy() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), 1..100)
    }

    proptest! {
        // a bit expensive, feel free to run it when changing the code above
        //#![proptest_config(ProptestConfig::with_cases(100000))]
        #[test]
        #[cfg_attr(miri, ignore)] // miri and proptest do not work well
        fn check_utf8_validation(values in binary_strategy()) {

            for offset in 0..values.len() - 1 {
                let offsets = vec![0, offset as i32, values.len() as i32].try_into().unwrap();

                let mut is_valid = std::str::from_utf8(&values[..offset]).is_ok();
                is_valid &= std::str::from_utf8(&values[offset..]).is_ok();

                assert_eq!(try_check_utf8::<i32, Offsets<i32>>(&offsets, &values).is_ok(), is_valid)
            }
        }
    }
}
