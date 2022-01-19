use crate::{bitmap::Bitmap, buffer::Buffer, datatypes::DataType};

use super::{Array, Offset};

mod iterator;

/// An equivalent representation of [`Vec<Option<String>>`] with the following properties:
/// * Clone is O(1)
/// * Slice is O(1)
/// * row-based operations are `O(N)`
/// # Safety
/// The following invariants are uphold:
/// * offsets.len() == lengths.len()
/// * offsets[i] + lengths[i] < values.len() for all i
/// * `&values[offsets[i]..offsets[i] + lengths[i]]` is valid utf8 for all i
#[derive(Debug, Clone)]
pub struct StringSequenceArray<O: Offset> {
    data_type: DataType,
    validity: Option<Bitmap>,
    values: Buffer<u8>,
    offsets: Buffer<O>,
    lengths: Buffer<O>,
}

impl<O: Offset> StringSequenceArray<O> {
    /// returns the [`DataType`] of this [`StringSequenceArray`]
    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// returns the length of this [`StringSequenceArray`]
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// The validity
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns the offsets that together with `lengths` slice `.values()` of values.
    #[inline]
    pub fn offsets(&self) -> &Buffer<O> {
        &self.offsets
    }

    /// Returns the offsets that together with `lengths` slice `.values()` of values.
    #[inline]
    pub fn lengths(&self) -> &Buffer<O> {
        &self.lengths
    }

    /// Returns all values in this array. Use `.offsets()` to slice them.
    #[inline]
    pub fn values(&self) -> &Buffer<u8> {
        &self.values
    }

    /// Returns the element at index `i`
    /// # Panics
    /// iff `i >= self.len()`
    #[inline]
    pub fn value(&self, i: usize) -> &str {
        assert!(i < self.len());
        // soundness: invariant verified above
        unsafe { self.value_unchecked(i) }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &str {
        let start = self.offsets.get_unchecked(i).to_usize();
        let length = self.lengths.get_unchecked(i).to_usize();

        // soundness: the invariant of the struct
        let slice = unsafe { self.values.get_unchecked(start..start + length) };

        // soundness: the invariant of the struct
        std::str::from_utf8_unchecked(slice)
    }
}

impl<O: Offset> StringSequenceArray<O> {
    /// Creates an empty [`StringSequenceArray`], i.e. whose `.len` is zero.
    pub fn new_empty(data_type: DataType) -> Self {
        Self {
            data_type,
            validity: None,
            lengths: vec![].into(),
            offsets: vec![].into(),
            values: vec![].into(),
        }
    }

    /// Creates an null [`BinaryArray`], i.e. whose `.null_count() == .len()`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self {
            data_type,
            validity: Some(Bitmap::new_zeroed(length)),
            lengths: vec![O::default(); length].into(),
            offsets: vec![O::default(); length].into(),
            values: vec![].into(),
        }
    }

    /// Creates a new [`StringSequenceArray`] with no validity
    pub fn from_values<T: AsRef<str>, I: Iterator<Item = T>>(iter: I) -> Self {
        let mut values = vec![];
        let mut offsets = Vec::with_capacity(iter.size_hint().0);
        let mut lengths = Vec::with_capacity(iter.size_hint().0);
        let mut length = O::default();
        for item in iter {
            let item = item.as_ref();
            values.extend_from_slice(item.as_bytes());

            let new_length = O::from_usize(item.len()).unwrap();
            lengths.push(new_length);
            length += new_length;
            offsets.push(length);
        }

        let data_type = if O::is_large() {
            DataType::LargeUtf8Sequence
        } else {
            DataType::Utf8Sequence
        };

        Self {
            data_type,
            validity: None,
            values: values.into(),
            offsets: offsets.into(),
            lengths: lengths.into(),
        }
    }

    /// Creates a new [`StringSequenceArray`] by slicing this [`StringSequenceArray`].
    /// # Implementation
    /// This function is `O(1)`: all data will be shared between both arrays.
    /// # Panics
    /// iff `offset + length > self.len()`.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Creates a new [`StringSequenceArray`] by slicing this [`StringSequenceArray`].
    /// # Implementation
    /// This function is `O(1)`: all data will be shared between both arrays.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|x| x.slice_unchecked(offset, length));
        let offsets = self.offsets.clone().slice_unchecked(offset, length);
        let lengths = self.lengths.clone().slice_unchecked(offset, length);
        Self {
            data_type: self.data_type.clone(),
            offsets,
            lengths,
            values: self.values.clone(),
            validity,
        }
    }

    /// Clones this [`StringSequenceArray`] with a different validity.
    /// # Panic
    /// Panics iff `validity.len() != self.len()`.
    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        if matches!(&validity, Some(bitmap) if bitmap.len() != self.len()) {
            panic!("validity's length must be equal to the array's length")
        }
        let mut arr = self.clone();
        arr.validity = validity;
        arr
    }
}

impl<O: Offset> Array for StringSequenceArray<O> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice_unchecked(offset, length))
    }

    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.with_validity(validity))
    }
}
