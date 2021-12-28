//! Contains [`Columns`], a container of [`Array`] where every array has the
//! same length.

use crate::array::Array;
use crate::error::{ArrowError, Result};

/// A vector of trait objects of [`Array`] where every item has
/// the same length, [`Columns::len`].
#[derive(Debug, Clone, PartialEq)]
pub struct Columns<A: std::borrow::Borrow<dyn Array>> {
    arrays: Vec<A>,
}

impl<A: std::borrow::Borrow<dyn Array>> Columns<A> {
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
            let len = arrays.first().unwrap().borrow().len();
            if arrays
                .iter()
                .map(|array| array.borrow())
                .any(|array| array.len() != len)
            {
                return Err(ArrowError::InvalidArgumentError(
                    "Columns require all its arrays to have an equal number of rows".to_string(),
                ));
            }
        }
        Ok(Self { arrays })
    }

    /// returns the [`Array`]s in [`Columns`]
    pub fn arrays(&self) -> &[A] {
        &self.arrays
    }

    /// returns the [`Array`]s in [`Columns`]
    pub fn columns(&self) -> &[A] {
        &self.arrays
    }

    /// returns the number of rows of every array
    pub fn len(&self) -> usize {
        self.arrays
            .first()
            .map(|x| x.borrow().len())
            .unwrap_or_default()
    }

    /// returns whether the columns have any rows
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Consumes [`Columns`] into its underlying arrays.
    /// The arrays are guaranteed to have the same length
    pub fn into_arrays(self) -> Vec<A> {
        self.arrays
    }
}

impl<A: std::borrow::Borrow<dyn Array>> From<Columns<A>> for Vec<A> {
    fn from(c: Columns<A>) -> Self {
        c.into_arrays()
    }
}

impl<A: std::borrow::Borrow<dyn Array>> std::ops::Deref for Columns<A> {
    type Target = [A];

    #[inline]
    fn deref(&self) -> &[A] {
        self.arrays()
    }
}
