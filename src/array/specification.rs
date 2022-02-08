use crate::error::{ArrowError, Result};
use crate::types::Offset;

pub fn check_offsets_minimal<O: Offset>(offsets: &[O], values_len: usize) -> usize {
    assert!(
        !offsets.is_empty(),
        "The length of the offset buffer must be larger than 1"
    );
    let len = offsets.len() - 1;

    let last_offset = offsets[len];
    let last_offset = last_offset.to_usize();

    assert_eq!(
        values_len, last_offset,
        "The length of the values must be equal to the last offset value"
    );
    len
}

/// # Panics iff:
/// * the `offsets` is not monotonically increasing, or
/// * any slice of `values` between two consecutive pairs from `offsets` is invalid `utf8`, or
/// * any offset is larger or equal to `values_len`.
pub fn check_offsets_and_utf8<O: Offset>(offsets: &[O], values: &[u8]) {
    try_check_offsets_and_utf8(offsets, values).unwrap()
}

/// # Panics iff:
/// * the `offsets` is not monotonically increasing, or
/// * any slice of `values` between two consecutive pairs from `offsets` is invalid `utf8`, or
/// * any offset is larger or equal to `values_len`.
pub fn try_check_offsets_and_utf8<O: Offset>(offsets: &[O], values: &[u8]) -> Result<()> {
    if values.is_ascii() {
        try_check_offsets(offsets, values.len())
    } else {
        simdutf8::basic::from_utf8(values)?;

        for window in offsets.windows(2) {
            let start = window[0].to_usize();
            let end = window[1].to_usize();

            // check monotonicity
            if start > end {
                return Err(ArrowError::oos("offsets must be monotonically increasing"));
            }

            let first = values.get(start);

            if let Some(&b) = first {
                // A valid code-point iff it does not start with 0b10xxxxxx
                // Bit-magic taken from `std::str::is_char_boundary`
                if (b as i8) < -0x40 {
                    return Err(ArrowError::oos("Non-valid char boundary detected"));
                }
            }
        }
        // check bounds
        if offsets
            .last()
            .map_or(false, |last| last.to_usize() > values.len())
        {
            return Err(ArrowError::oos("offsets must not exceed values length"));
        };

        Ok(())
    }
}

/// # Panics iff:
/// * the `offsets` is not monotonically increasing, or
/// * any offset is larger or equal to `values_len`.
pub fn check_offsets<O: Offset>(offsets: &[O], values_len: usize) {
    try_check_offsets(offsets, values_len).unwrap()
}

/// Checks that `offsets` is monotonically increasing, and all offsets are less than or equal to
/// `values_len`.
pub fn try_check_offsets<O: Offset>(offsets: &[O], values_len: usize) -> Result<()> {
    if offsets.windows(2).any(|window| window[0] > window[1]) {
        Err(ArrowError::oos("offsets must be monotonically increasing"))
    } else if offsets
        .last()
        .map_or(false, |last| last.to_usize() > values_len)
    {
        Err(ArrowError::oos("offsets must not exceed values length"))
    } else {
        Ok(())
    }
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
                let offsets = vec![0, offset as i32, values.len() as i32];

                let mut is_valid = std::str::from_utf8(&values[..offset]).is_ok();
                is_valid &= std::str::from_utf8(&values[offset..]).is_ok();

                assert_eq!(try_check_offsets_and_utf8::<i32>(&offsets, &values).is_ok(), is_valid)
            }
        }
    }
}
