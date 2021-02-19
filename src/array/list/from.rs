use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{Array, Builder, Offset, ToArray},
    buffer::{Bitmap, Buffer, MutableBitmap, MutableBuffer},
    datatypes::DataType,
};

use super::ListArray;

#[derive(Debug)]
pub struct ListPrimitive<O: Offset, A: ToArray> {
    offsets: Buffer<O>,
    values: A,
    validity: Option<Bitmap>,
}

impl<O: Offset, A: ToArray> ListPrimitive<O, A> {
    pub fn to(self, data_type: DataType) -> ListArray<O> {
        let values = self.values.to_arc(ListArray::<O>::get_child(&data_type));
        ListArray::from_data(data_type, self.offsets, values, self.validity)
    }
}

pub fn list_from_iter<O, B, T, P, I>(iter: I) -> (Buffer<O>, B, Option<Bitmap>)
where
    O: Offset,
    B: Builder<T>,
    P: AsRef<[Option<T>]> + IntoIterator<Item = Option<T>>,
    I: IntoIterator<Item = Option<P>>,
{
    let iterator = iter.into_iter();
    let (lower, _) = iterator.size_hint();

    let mut offsets = MutableBuffer::<O>::with_capacity(lower + 1);
    let mut length_so_far = O::zero();
    offsets.push(length_so_far);

    let mut nulls = MutableBitmap::with_capacity(lower);

    let mut values = B::with_capacity(0);
    iterator
        .filter_map(|maybe_slice| {
            // regardless of whether the item is Some, the offsets and null buffers must be updated.
            match &maybe_slice {
                Some(x) => {
                    length_so_far += O::from_usize(x.as_ref().len()).unwrap();
                    nulls.push(true);
                }
                None => nulls.push(false),
            };
            offsets.push(length_so_far);
            maybe_slice
        })
        .flatten()
        .for_each(|x| values.push(x.as_ref()));

    (offsets.into(), values, nulls.into())
}

impl<O, B, T, P> FromIterator<Option<P>> for ListPrimitive<O, B>
where
    O: Offset,
    B: Builder<T>,
    P: AsRef<[Option<T>]> + IntoIterator<Item = Option<T>>,
{
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        let (offsets, values, validity) = list_from_iter(iter);
        Self {
            offsets,
            values,
            validity,
        }
    }
}

impl<O: Offset, B: ToArray> ToArray for ListPrimitive<O, B> {
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        Arc::new(self.to(data_type.clone()))
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{Primitive, PrimitiveArray};

    use super::*;

    #[test]
    fn primitive() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let a: ListPrimitive<i32, Primitive<i32>> = data.into_iter().collect();
        let a = a.to(ListArray::<i32>::default_datatype(DataType::Int32));
        let a = a.value(0);
        let a = a.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

        let expected =
            Primitive::<i32>::from(vec![Some(1i32), Some(2), Some(3)]).to(DataType::Int32);
        assert_eq!(a, &expected)
    }
}
