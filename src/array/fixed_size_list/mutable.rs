use std::sync::Arc;

use crate::{
    array::{
        Array, MutableArray, MutableBinaryArray, MutablePrimitiveArray, MutableUtf8Array, Offset,
    },
    bitmap::MutableBitmap,
    datatypes::DataType,
    error::{ArrowError, Result},
    types::NativeType,
};

use super::FixedSizeListArray;

/// The mutable version of [`FixedSizeListArray`].
#[derive(Debug)]
pub struct MutableFixedSizeListArray<M: MutableArray> {
    data_type: DataType,
    size: usize,
    values: M,
    validity: Option<MutableBitmap>,
}

impl<M: MutableArray> From<MutableFixedSizeListArray<M>> for FixedSizeListArray {
    fn from(mut other: MutableFixedSizeListArray<M>) -> Self {
        FixedSizeListArray::from_data(
            other.data_type,
            other.values.as_arc(),
            other.validity.map(|x| x.into()),
        )
    }
}

impl<M: MutableArray> MutableFixedSizeListArray<M> {
    pub fn new(values: M, size: usize) -> Self {
        let data_type = FixedSizeListArray::default_datatype(values.data_type().clone(), size);
        assert_eq!(values.len(), 0);
        Self {
            size,
            data_type,
            values,
            validity: None,
        }
    }

    pub fn mut_values(&mut self) -> &mut M {
        &mut self.values
    }

    pub fn values(&self) -> &M {
        &self.values
    }

    fn init_validity(&mut self) {
        self.validity = Some(MutableBitmap::from_trusted_len_iter(
            std::iter::repeat(true)
                .take(self.values.len() - 1)
                .chain(std::iter::once(false)),
        ))
    }
}

impl<M: MutableArray + 'static> MutableArray for MutableFixedSizeListArray<M> {
    fn len(&self) -> usize {
        self.values.len() / self.size
    }

    fn validity(&self) -> &Option<MutableBitmap> {
        &self.validity
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(FixedSizeListArray::from_data(
            self.data_type.clone(),
            self.values.as_arc(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ))
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

    fn push_null(&mut self) {
        (0..self.size).for_each(|_| {
            self.values.push_null();
        });
        if let Some(validity) = &mut self.validity {
            validity.push(false)
        } else {
            self.init_validity()
        }
    }
}

impl<T: NativeType> MutableFixedSizeListArray<MutablePrimitiveArray<T>> {
    pub fn try_from_iter<P: IntoIterator<Item = Option<T>>, I: IntoIterator<Item = Option<P>>>(
        iter: I,
        size: usize,
        data_type: DataType,
    ) -> Result<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let array = MutablePrimitiveArray::<T>::with_capacity_from(lower * size, data_type);
        let mut array = MutableFixedSizeListArray::new(array, size);
        for items in iterator {
            if let Some(items) = items {
                let values = array.mut_values();
                let len = values.len();
                values.extend(items);
                if values.len() - len != size {
                    return Err(ArrowError::InvalidArgumentError(
                        "A FixedSizeList must have all its values with the same size".to_string(),
                    ));
                };
            } else {
                array.push_null();
            }
        }
        Ok(array)
    }
}

macro_rules! impl_offsets {
    ($mutable:ident, $type:ty) => {
        impl<O: Offset> MutableFixedSizeListArray<$mutable<O>> {
            pub fn try_from_iter<
                T: AsRef<$type>,
                P: IntoIterator<Item = Option<T>>,
                I: IntoIterator<Item = Option<P>>,
            >(
                iter: I,
                size: usize,
            ) -> Result<Self> {
                let iterator = iter.into_iter();
                let (lower, _) = iterator.size_hint();
                let array = $mutable::<O>::with_capacity(lower * size);
                let mut array = MutableFixedSizeListArray::new(array, size);
                for items in iterator {
                    if let Some(items) = items {
                        let values = array.mut_values();
                        let len = values.len();
                        values.extend(items);
                        if values.len() - len != size {
                            return Err(ArrowError::InvalidArgumentError(
                                "A FixedSizeList must have all its values with the same size"
                                    .to_string(),
                            ));
                        };
                    } else {
                        array.push_null();
                    }
                }
                Ok(array)
            }
        }
    };
}

impl_offsets!(MutableUtf8Array, str);
impl_offsets!(MutableBinaryArray, [u8]);

#[cfg(test)]
mod tests {
    use crate::array::Int32Array;

    use super::*;

    #[test]
    fn primitive() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            Some(vec![None, None, None]),
            Some(vec![Some(4), None, Some(6)]),
        ];

        let list: FixedSizeListArray =
            MutableFixedSizeListArray::<MutablePrimitiveArray<i32>>::try_from_iter(
                data,
                3,
                DataType::Int32,
            )
            .unwrap()
            .into();

        let a = list.value(0);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![Some(1i32), Some(2), Some(3)]);
        assert_eq!(a, &expected);

        let a = list.value(1);
        let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![None, None, None]);
        assert_eq!(a, &expected)
    }
}
