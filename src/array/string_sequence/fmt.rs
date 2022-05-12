use std::fmt::{Debug, Formatter, Result, Write};

use super::super::fmt::write_vec;
use super::super::Offset;
use super::StringSequenceArray;

pub fn write_value<O: Offset, W: Write>(
    array: &StringSequenceArray<O>,
    index: usize,
    f: &mut W,
) -> Result {
    write!(f, "{}", array.value(index))
}

impl<O: Offset> Debug for StringSequenceArray<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let writer = |f: &mut Formatter, index| write_value(self, index, f);

        let head = if O::is_large() {
            "LargeStringSequenceArray"
        } else {
            "StringSequenceArray"
        };
        write!(f, "{}", head)?;
        write_vec(f, writer, self.validity(), self.len(), "None", false)
    }
}
