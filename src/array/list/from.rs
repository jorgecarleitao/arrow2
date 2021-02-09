use std::{iter::FromIterator, sync::Arc};

use crate::{
    array::{primitive::ToArray, Array, Offset, Primitive},
    buffer::{Bitmap, Buffer, MutableBitmap, MutableBuffer, NativeType},
    datatypes::DataType,
};

use super::ListArray;

#[derive(Debug)]
pub struct ListPrimitive<O: Offset, A: ToArray> {
    offsets: Buffer<O>,
    values: A,
    validity: Option<Bitmap>,
}

impl<T: NativeType, O: Offset> ListPrimitive<O, Primitive<T>> {
    pub fn to(self, data_type: DataType) -> ListArray<O> {
        let values = self.values.to_arc(ListArray::<O>::get_child(&data_type));
        ListArray::from_data(data_type, self.offsets, values, self.validity)
    }
}

pub fn list_from_iter_primitive<O, T, P, I>(iter: I) -> (Buffer<O>, Primitive<T>, Option<Bitmap>)
where
    O: Offset,
    T: NativeType,
    P: AsRef<[Option<T>]> + IntoIterator<Item = Option<T>>,
    I: IntoIterator<Item = Option<P>>,
{
    let iterator = iter.into_iter();
    let (lower, _) = iterator.size_hint();

    let mut offsets = MutableBuffer::<O>::with_capacity(lower + 1);
    let mut length_so_far = O::zero();
    offsets.push(length_so_far);

    let mut nulls = MutableBitmap::with_capacity(lower);

    let values: Primitive<T> = iterator
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
        .collect();

    let bitmap = if nulls.null_count() > 0 {
        Some(nulls.into())
    } else {
        None
    };

    (offsets.into(), values, bitmap)
}

impl<O, T, P> FromIterator<Option<P>> for ListPrimitive<O, Primitive<T>>
where
    O: Offset,
    T: NativeType,
    P: AsRef<[Option<T>]> + IntoIterator<Item = Option<T>>,
{
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        let (offsets, values, validity) = list_from_iter_primitive(iter);
        Self {
            offsets,
            values,
            validity,
        }
    }
}

impl<T: NativeType, O: Offset> ToArray for ListPrimitive<O, Primitive<T>> {
    fn to_arc(self, data_type: &DataType) -> Arc<dyn Array> {
        let list = self.to(data_type.clone());
        Arc::new(list)
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
