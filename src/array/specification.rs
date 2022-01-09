use crate::{
    error::{ArrowError, Result},
    types::Offset,
};

pub fn check_offsets_minimal<O: Offset>(offsets: &[O], values_len: usize) -> Result<usize> {
    if offsets.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "The offsets of a variable-sized array must contain at least one element (usually 0)"
                .to_string(),
        ));
    }
    let len = offsets.len() - 1;

    let last_offset = offsets[len];
    let last_offset = last_offset.to_usize();
    if last_offset > values_len {
        return Err(ArrowError::InvalidArgumentError(
            "The last offset of a variable-sized array is not equal to the length of the values"
                .to_string(),
        ));
    }

    Ok(len)
}

/// # Errors iff:
/// * `offsets` is empty
/// * `offsets` is not monotonically increasing, or
/// * any slice of `values` between two consecutive pairs from `offsets` is invalid `utf8`, or
/// * any offset is larger or equal to `values_len`.
pub fn check_offsets_and_utf8<O: Offset>(offsets: &[O], values: &[u8]) -> Result<()> {
    const SIMD_CHUNK_SIZE: usize = 64;

    if values.is_ascii() {
        check_offsets(offsets, values.len())
    } else {
        if offsets.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "The offsets of a variable-sized array must contain at least one element (usually 0)"
                    .to_string(),
            ));
        }

        offsets.windows(2).try_for_each(|window| {
            let start = window[0].to_usize();
            let end = window[1].to_usize();
            // assert monotonicity
            assert!(start <= end);
            // assert bounds
            let slice = &values[start..end];

            // Fast ASCII check per item
            if slice.len() < SIMD_CHUNK_SIZE && slice.is_ascii() {
                return Ok(());
            }

            // assert utf8
            simdutf8::basic::from_utf8(slice).map(|_| ()).map_err(|_| {
                ArrowError::InvalidArgumentError("A non-utf8 string was passed.".to_string())
            })
        })
    }
}

/// # Errors
/// Iff:
/// * `offsets` is empty
/// * `offsets` is not monotonically increasing, or
/// * any offset is larger or equal to `values_len`.
pub fn check_offsets<O: Offset>(offsets: &[O], values_len: usize) -> Result<()> {
    if offsets.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "The offsets of a variable-sized array must contain at least one element (usually 0)"
                .to_string(),
        ));
    }

    let mut last = offsets[0];
    let are_monotone = offsets.iter().skip(1).all(|&end| {
        let monotone = last <= end;
        last = end;
        monotone
    });
    if !are_monotone {
        return Err(ArrowError::InvalidArgumentError(
            "The offsets of a variable-sized array are not monotonically increasing".to_string(),
        ));
    }
    // assert bounds
    if last.to_usize() > values_len {
        return Err(ArrowError::InvalidArgumentError(
            "The last offset of a variable-sized array is not equal to the length of the values"
                .to_string(),
        ));
    }
    Ok(())
}
