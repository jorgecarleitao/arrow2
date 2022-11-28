//! Contains the [`ValidityOffsets'] struct and implementations.
use crate::array::specification::try_check_offsets;
use crate::array::Offset;
use crate::buffer::Buffer;
use crate::error::{Error, Result};

/// Offsets that have the invariant that they
/// are monotonically increasing.
pub struct ValidOffsets<O: Offset>(Buffer<O>);

impl<O: Offset> ValidOffsets<O> {
    /// Try to create a new [`ValidOffsets`] buffer by checking the offsets.
    pub fn try_new(offsets: Buffer<O>) -> Result<Self> {
        match offsets.last() {
            None => Err(Error::oos("offsets must have at least one element")),
            Some(last) => {
                try_check_offsets(offsets.as_slice(), last.to_usize())?;
                Ok(ValidOffsets(offsets))
            }
        }
    }

    /// Create a new [`ValidOffsets`] buffer.
    ///
    /// # Safety
    ///
    /// The offsets must be monotonically increasing.
    pub unsafe fn new_unchecked(offsets: Buffer<O>) -> Result<Self> {
        if offsets.first().is_none() {
            Err(Error::oos("offsets must have at least one element"))
        } else {
            Ok(ValidOffsets(offsets))
        }
    }
}

impl<O: Offset> From<ValidOffsets<O>> for Buffer<O> {
    fn from(vo: ValidOffsets<O>) -> Self {
        vo.0
    }
}

impl<O: Offset> AsRef<[O]> for ValidOffsets<O> {
    fn as_ref(&self) -> &[O] {
        self.0.as_slice()
    }
}
