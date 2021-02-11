mod serialize;
mod writer;

pub use serialize::{write, write_dictionary};
pub use writer::{FileWriter, StreamWriter};

/// Calculate an 8-byte boundary and return the number of bytes needed to pad to 8 bytes
#[inline]
pub(crate) fn pad_to_8(len: u32) -> usize {
    (((len + 7) & !7) - len) as usize
}
