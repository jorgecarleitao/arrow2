use std::sync::Arc;

use crate::{
    array::{
        physical_binary::{extend_validity, try_extend_offsets},
        specification::try_check_offsets,
        Array, MutableArray, Offset, TryExtend, TryExtendFromSelf, TryPush,
    },
    bitmap::MutableBitmap,
    datatypes::{DataType, Field},
    error::{Error, Result},
    trusted_len::TrustedLen,
};

use super::ListArray;

/// The mutable version of [`ListArray`].
#[derive(Debug, Clone)]
pub struct MutableListArray<O: Offset, M: MutableArray> {
    data_type: DataType,
    offsets: Vec<O>,
    values: M,
    validity: Option<MutableBitmap>,
}

impl<O: Offset, M: MutableArray + Default> MutableListArray<O, M> {
    /// Creates a new empty [`MutableListArray`].
    pub fn new() -> Self {
        let values = M::default();
        let data_type = ListArray::<O>::default_datatype(values.data_type().clone());
        Self::new_from(values, data_type, 0)
    }

    /// Creates a new [`MutableListArray`] with a capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let values = M::default();
        let data_type = ListArray::<O>::default_datatype(values.data_type().clone());

        let mut offsets = Vec::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());
        Self {
            data_type,
            offsets,
            values,
            validity: None,
        }
    }
}

impl<O: Offset, M: MutableArray + Default> Default for MutableListArray<O, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset, M: MutableArray> From<MutableListArray<O, M>> for ListArray<O> {
    fn from(mut other: MutableListArray<O, M>) -> Self {
        // Safety:
        // MutableListArray has monotonically increasing offsets
        unsafe {
            ListArray::new_unchecked(
                other.data_type,
                other.offsets.into(),
                other.values.as_box(),
                other.validity.map(|x| x.into()),
            )
        }
    }
}

impl<O, M, I, T> TryExtend<Option<I>> for MutableListArray<O, M>
where
    O: Offset,
    M: MutableArray + TryExtend<Option<T>>,
    I: IntoIterator<Item = Option<T>>,
{
    fn try_extend<II: IntoIterator<Item = Option<I>>>(&mut self, iter: II) -> Result<()> {
        let iter = iter.into_iter();
        self.reserve(iter.size_hint().0);
        for items in iter {
            self.try_push(items)?;
        }
        Ok(())
    }
}

impl<O, M, I, T> TryPush<Option<I>> for MutableListArray<O, M>
where
    O: Offset,
    M: MutableArray + TryExtend<Option<T>>,
    I: IntoIterator<Item = Option<T>>,
{
    #[inline]
    fn try_push(&mut self, item: Option<I>) -> Result<()> {
        if let Some(items) = item {
            let values = self.mut_values();
            values.try_extend(items)?;
            self.try_push_valid()?;
        } else {
            self.push_null();
        }
        Ok(())
    }
}

impl<O, M> TryExtendFromSelf for MutableListArray<O, M>
where
    O: Offset,
    M: MutableArray + TryExtendFromSelf,
{
    fn try_extend_from_self(&mut self, other: &Self) -> Result<()> {
        extend_validity(self.len(), &mut self.validity, &other.validity);

        self.values.try_extend_from_self(&other.values)?;

        try_extend_offsets(&mut self.offsets, &other.offsets)
    }
}

impl<O: Offset, M: MutableArray> MutableListArray<O, M> {
    /// Creates a new [`MutableListArray`] from a [`MutableArray`] and capacity.
    pub fn new_from(values: M, data_type: DataType, capacity: usize) -> Self {
        let mut offsets = Vec::<O>::with_capacity(capacity + 1);
        offsets.push(O::default());
        assert_eq!(values.len(), 0);
        ListArray::<O>::get_child_field(&data_type);
        Self {
            data_type,
            offsets,
            values,
            validity: None,
        }
    }

    /// Creates a new [`MutableListArray`] from a [`MutableArray`].
    pub fn new_with_field(values: M, name: &str, nullable: bool) -> Self {
        let field = Box::new(Field::new(name, values.data_type().clone(), nullable));
        let data_type = if O::IS_LARGE {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        };
        Self::new_from(values, data_type, 0)
    }

    /// Creates a new [`MutableListArray`] from a [`MutableArray`] and capacity.
    pub fn new_with_capacity(values: M, capacity: usize) -> Self {
        let data_type = ListArray::<O>::default_datatype(values.data_type().clone());
        Self::new_from(values, data_type, capacity)
    }

    #[inline]
    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    pub fn try_push_valid(&mut self) -> Result<()> {
        let size = self.values.len();
        let size = O::from_usize(size).ok_or(Error::Overflow)?;
        assert!(size >= *self.offsets.last().unwrap());

        self.offsets.push(size);
        if let Some(validity) = &mut self.validity {
            validity.push(true)
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) {
        self.offsets.push(self.last_offset());
        match &mut self.validity {
            Some(validity) => validity.push(false),
            None => self.init_validity(),
        }
    }

    /// Expand this array, using elements from the underlying backing array.
    /// Assumes the expansion begins at the highest previous offset, or zero if
    /// this [MutableListArray] is currently empty.
    ///
    /// Panics if:
    /// - the new offsets are not in monotonic increasing order.
    /// - any new offset is not in bounds of the backing array.
    /// - the passed iterator has no upper bound.
    pub(crate) fn extend_offsets<II>(&mut self, expansion: II)
    where
        II: TrustedLen<Item = Option<O>>,
    {
        let current_len = self.offsets.len();
        let (_, upper) = expansion.size_hint();
        let upper = upper.expect("iterator must have upper bound");
        if current_len == 0 && upper > 0 {
            self.offsets.push(O::zero());
        }
        // safety: checked below
        unsafe { self.unsafe_extend_offsets(expansion) };
        if self.offsets.len() > current_len {
            // check all inserted offsets
            try_check_offsets(&self.offsets[current_len..], self.values.len())
                .expect("invalid offsets");
        }
        // else expansion is empty, and this is trivially safe.
    }

    /// Expand this array, using elements from the underlying backing array.
    /// Assumes the expansion begins at the highest previous offset, or zero if
    /// this [MutableListArray] is currently empty.
    ///
    /// # Safety
    ///
    /// Assumes that `offsets` are in order, and do not overrun the underlying
    /// `values` backing array.
    ///
    /// Also assumes the expansion begins at the highest previous offset, or
    /// zero if the array is currently empty.
    ///
    /// Panics if the passed iterator has no upper bound.
    pub(crate) unsafe fn unsafe_extend_offsets<II>(&mut self, expansion: II)
    where
        II: TrustedLen<Item = Option<O>>,
    {
        let (_, upper) = expansion.size_hint();
        let upper = upper.expect("iterator must have upper bound");
        let final_size = self.len() + upper;
        self.offsets.reserve(upper);

        for item in expansion {
            match item {
                Some(offset) => {
                    self.offsets.push(offset);
                    if let Some(validity) = &mut self.validity {
                        validity.push(true);
                    }
                }
                None => self.push_null(),
            }

            if let Some(validity) = &mut self.validity {
                if validity.capacity() < final_size {
                    validity.reserve(final_size - validity.capacity());
                }
            }
        }
    }

    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// The values
    pub fn mut_values(&mut self) -> &mut M {
        &mut self.values
    }

    /// The offsets
    pub fn offsets(&self) -> &Vec<O> {
        &self.offsets
    }

    /// The values
    pub fn values(&self) -> &M {
        &self.values
    }

    #[inline]
    fn last_offset(&self) -> O {
        *self.offsets.last().unwrap()
    }

    fn init_validity(&mut self) {
        let len = self.offsets.len() - 1;

        let mut validity = MutableBitmap::with_capacity(self.offsets.capacity());
        validity.extend_constant(len, true);
        validity.set(len - 1, false);
        self.validity = Some(validity)
    }

    /// Converts itself into an [`Array`].
    pub fn into_arc(self) -> Arc<dyn Array> {
        let a: ListArray<O> = self.into();
        Arc::new(a)
    }

    /// converts itself into [`Box<dyn Array>`]
    pub fn into_box(self) -> Box<dyn Array> {
        let a: ListArray<O> = self.into();
        Box::new(a)
    }

    /// Reserves `additional` slots.
    pub fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
        if let Some(x) = self.validity.as_mut() {
            x.reserve(additional)
        }
    }

    /// Shrinks the capacity of the [`MutableListArray`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        self.offsets.shrink_to_fit();
        if let Some(validity) = &mut self.validity {
            validity.shrink_to_fit()
        }
    }
}

impl<O: Offset, M: MutableArray + 'static> MutableArray for MutableListArray<O, M> {
    fn len(&self) -> usize {
        MutableListArray::len(self)
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        // Safety:
        // MutableListArray has monotonically increasing offsets
        Box::new(unsafe {
            ListArray::new_unchecked(
                self.data_type.clone(),
                std::mem::take(&mut self.offsets).into(),
                self.values.as_box(),
                std::mem::take(&mut self.validity).map(|x| x.into()),
            )
        })
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        // Safety:
        // MutableListArray has monotonically increasing offsets
        Arc::new(unsafe {
            ListArray::new_unchecked(
                self.data_type.clone(),
                std::mem::take(&mut self.offsets).into(),
                self.values.as_box(),
                std::mem::take(&mut self.validity).map(|x| x.into()),
            )
        })
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    #[inline]
    fn push_null(&mut self) {
        self.push_null()
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve(additional)
    }

    fn shrink_to_fit(&mut self) {
        self.shrink_to_fit();
    }
}
