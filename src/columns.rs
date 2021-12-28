//! Contains [`Columns`], a container [`Array`] where all arrays have the
//! same length.
use std::sync::Arc;

use crate::array::Array;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

/// A vector of [`Array`] where every array has the same length.
#[derive(Debug, Clone, PartialEq)]
pub struct Columns<A: AsRef<dyn Array>> {
    arrays: Vec<A>,
}

impl<A: AsRef<dyn Array>> Columns<A> {
    /// Creates a new [`Columns`].
    /// # Panic
    /// Iff the arrays do not have the same length
    pub fn new(arrays: Vec<A>) -> Self {
        Self::try_new(arrays).unwrap()
    }

    /// Creates a new [`Columns`].
    /// # Error
    /// Iff the arrays do not have the same length
    pub fn try_new(arrays: Vec<A>) -> Result<Self> {
        if !arrays.is_empty() {
            let len = arrays.first().unwrap().as_ref().len();
            if arrays
                .iter()
                .map(|array| array.as_ref())
                .any(|array| array.len() != len)
            {
                return Err(ArrowError::InvalidArgumentError(
                    "Columns require all its arrays to have an equal number of rows".to_string(),
                ));
            }
        }
        Ok(Self { arrays })
    }

    /// returns the [`Array`]s in [`Columns`].
    pub fn arrays(&self) -> &[A] {
        &self.arrays
    }

    /// returns the length (number of rows)
    pub fn len(&self) -> usize {
        self.arrays
            .first()
            .map(|x| x.as_ref().len())
            .unwrap_or_default()
    }

    /// Consumes [`Columns`] into its underlying arrays.
    /// The arrays are guaranteed to have the same length
    pub fn into_arrays(self) -> Vec<A> {
        self.arrays
    }
}

impl<A: AsRef<dyn Array>> From<Columns<A>> for Vec<A> {
    fn from(c: Columns<A>) -> Self {
        c.into_arrays()
    }
}

impl<A: AsRef<dyn Array>> std::ops::Deref for Columns<A> {
    type Target = [A];

    #[inline]
    fn deref(&self) -> &[A] {
        self.arrays()
    }
}

impl From<RecordBatch> for Columns<Arc<dyn Array>> {
    fn from(batch: RecordBatch) -> Self {
        Self {
            arrays: batch.into_inner().0,
        }
    }
}
