use csv::ByteRecord;

use crate::array::BooleanArray;

/// default behavior is infalible: `None` if unable to parse
pub trait BooleanParser<E> {
    fn parse(&self, string: &[u8], _: usize) -> Result<Option<bool>, E> {
        Ok(if string.eq_ignore_ascii_case(b"false") {
            Some(false)
        } else if string.eq_ignore_ascii_case(b"true") {
            Some(true)
        } else {
            None
        })
    }
}

// parses a specific column (col_idx) into an Arrow Array.
pub fn new_boolean_array<E, P: BooleanParser<E>>(
    line_number: usize,
    rows: &[ByteRecord],
    col_idx: usize,
    parser: &P,
) -> Result<BooleanArray, E> {
    let iter = rows
        .iter()
        .enumerate()
        .map(|(row_index, row)| match row.get(col_idx) {
            Some(s) => {
                if s.is_empty() {
                    return Ok(None);
                }
                parser.parse(s, row_index + line_number)
            }
            None => Ok(None),
        });
    unsafe { BooleanArray::try_from_trusted_len_iter(iter) }
}
