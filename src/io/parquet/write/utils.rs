use crate::bitmap::Bitmap;
use parquet2::encoding::hybrid_rle::encode;

use crate::error::Result;

#[inline]
fn encode_iter<I: Iterator<Item = bool>>(iter: I) -> Result<Vec<u8>> {
    let mut buffer = std::io::Cursor::new(vec![0; 4]);
    buffer.set_position(4);
    encode(&mut buffer, iter)?;
    let mut buffer = buffer.into_inner();
    let length = buffer.len() - 4;
    // todo: pay this small debt (loop?)
    let length = length.to_le_bytes();
    buffer[0] = length[0];
    buffer[1] = length[1];
    buffer[2] = length[2];
    buffer[3] = length[3];
    Ok(buffer)
}

/// writes the def levels to a `Vec<u8>` and returns it.
/// Note that this function
#[inline]
pub fn write_def_levels(
    is_optional: bool,
    validity: &Option<Bitmap>,
    len: usize,
) -> Result<Vec<u8>> {
    // encode def levels
    match (is_optional, validity) {
        (true, Some(validity)) => encode_iter(validity.iter()),
        (true, None) => encode_iter(std::iter::repeat(true).take(len)),
        _ => Ok(vec![]), // is required => no def levels
    }
}
