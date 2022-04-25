use parquet2::statistics::{FixedLenStatistics, Statistics as ParquetStatistics};

use crate::array::*;
use crate::error::Result;

fn convert(value: &[u8], n: usize) -> i128 {
    // Copy the fixed-size byte value to the start of a 16 byte stack
    // allocated buffer, then use an arithmetic right shift to fill in
    // MSBs, which accounts for leading 1's in negative (two's complement)
    // values.
    let mut bytes = [0u8; 16];
    bytes[..n].copy_from_slice(value);
    i128::from_be_bytes(bytes) >> (8 * (16 - n))
}

pub(super) fn push_i128(
    from: Option<&dyn ParquetStatistics>,
    n: usize,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i128>>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i128>>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<FixedLenStatistics>().unwrap());

    min.push(from.and_then(|s| s.min_value.as_deref().map(|x| convert(x, n))));
    max.push(from.and_then(|s| s.max_value.as_deref().map(|x| convert(x, n))));

    Ok(())
}

pub(super) fn push(
    from: Option<&dyn ParquetStatistics>,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutableFixedSizeBinaryArray>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutableFixedSizeBinaryArray>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<FixedLenStatistics>().unwrap());
    min.push(from.and_then(|s| s.min_value.as_ref()));
    max.push(from.and_then(|s| s.max_value.as_ref()));
    Ok(())
}
