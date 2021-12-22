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
    const SIMD_CHUNK_SIZE: usize = 64;

    if values.is_ascii() {
        check_offsets(offsets, values.len());
    } else {
        offsets.windows(2).for_each(|window| {
            let start = window[0].to_usize();
            let end = window[1].to_usize();
            // assert monotonicity
            assert!(start <= end);
            // assert bounds
            let slice = &values[start..end];

            // Fast ASCII check per item
            if slice.len() < SIMD_CHUNK_SIZE && slice.is_ascii() {
                return;
            }

            // assert utf8
            simdutf8::basic::from_utf8(slice).expect("A non-utf8 string was passed.");
        });
    }
}

/// # Panics iff:
/// * the `offsets` is not monotonically increasing, or
/// * any offset is larger or equal to `values_len`.
pub fn check_offsets<O: Offset>(offsets: &[O], values_len: usize) {
    if offsets.is_empty() {
        return;
    }

    let mut last = offsets[0];
    // assert monotonicity
    assert!(offsets.iter().skip(1).all(|&end| {
        let monotone = last <= end;
        last = end;
        monotone
    }));
    // assert bounds
    assert!(last.to_usize() <= values_len);
}
