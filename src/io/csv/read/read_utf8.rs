use super::ByteRecord;

use crate::array::{Offset, Utf8Array};

pub trait Utf8Parser<E> {
    fn parse<'a>(&self, bytes: &'a [u8], _: usize) -> Result<Option<&'a str>, E> {
        // default behavior is infalible: `None` if unable to parse
        Ok(std::str::from_utf8(bytes).ok())
    }
}

pub fn new_utf8_array<O: Offset, E, P: Utf8Parser<E>>(
    line_number: usize,
    rows: &[ByteRecord],
    col_idx: usize,
    parser: &P,
) -> Result<Utf8Array<O>, E> {
    let iter = rows
        .iter()
        .enumerate()
        .map(|(row_index, row)| match row.get(col_idx) {
            Some(s) => parser.parse(s, line_number + row_index),
            None => Ok(None),
        });
    // Soundness: slice is trusted len.
    unsafe { Utf8Array::<O>::try_from_trusted_len_iter(iter) }
}
