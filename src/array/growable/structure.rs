use std::sync::Arc;

use crate::{
    array::{Array, StructArray},
    bitmap::MutableBitmap,
};

use super::{
    make_growable,
    utils::{build_extend_null_bits, ExtendNullBits},
    Growable,
};

/// A growable PrimitiveArray
pub struct GrowableStruct<'a> {
    arrays: Vec<&'a StructArray>,
    validity: MutableBitmap,
    values: Vec<Box<dyn Growable<'a> + 'a>>,
    // function used to extend nulls from arrays. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a> GrowableStruct<'a> {
    /// # Panics
    /// This function panics if any of the `arrays` is not downcastable to `PrimitiveArray<T>`.
    pub fn new(arrays: &[&'a dyn Array], mut use_validity: bool, capacity: usize) -> Self {
        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(*array, use_validity))
            .collect();

        let arrays = arrays
            .iter()
            .map(|array| array.as_any().downcast_ref::<StructArray>().unwrap())
            .collect::<Vec<_>>();

        // ([field1, field2], [field3, field4]) -> ([field1, field3], [field2, field3])
        let values = (0..arrays[0].values().len())
            .map(|i| {
                make_growable(
                    &arrays
                        .iter()
                        .map(|x| x.values()[i].as_ref())
                        .collect::<Vec<_>>(),
                    use_validity,
                    capacity,
                )
            })
            .collect::<Vec<Box<dyn Growable>>>();

        Self {
            arrays,
            values,
            validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
        }
    }

    fn to(&mut self) -> StructArray {
        let validity = std::mem::take(&mut self.validity);
        let values = std::mem::take(&mut self.values);
        let values = values.into_iter().map(|mut x| x.as_arc()).collect();

        StructArray::from_data(self.arrays[0].fields().to_vec(), values, validity.into())
    }
}

impl<'a> Growable<'a> for GrowableStruct<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.validity, start, len);

        let array = self.arrays[index];
        if array.null_count() == 0 {
            self.values
                .iter_mut()
                .for_each(|child| child.extend(index, start, len))
        } else {
            (start..start + len).for_each(|i| {
                if array.is_valid(i) {
                    self.values
                        .iter_mut()
                        .for_each(|child| child.extend(index, i, 1))
                } else {
                    self.values
                        .iter_mut()
                        .for_each(|child| child.extend_validity(1))
                }
            })
        }
    }

    fn extend_validity(&mut self, additional: usize) {
        self.values
            .iter_mut()
            .for_each(|child| child.extend_validity(additional));
        self.validity.extend_constant(additional, false);
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a> From<GrowableStruct<'a>> for StructArray {
    fn from(val: GrowableStruct<'a>) -> Self {
        let values = val.values.into_iter().map(|mut x| x.as_arc()).collect();

        StructArray::from_data(val.arrays[0].fields().to_vec(), values, val.validity.into())
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use crate::array::{Primitive, Utf8Array};
    use crate::bitmap::Bitmap;
    use crate::datatypes::{DataType, Field};

    use super::*;

    fn some_values() -> (Vec<Field>, Vec<Arc<dyn Array>>) {
        let strings: Arc<dyn Array> = Arc::new(Utf8Array::<i32>::from_iter(vec![
            Some("a"),
            Some("aa"),
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: Arc<dyn Array> = Arc::new(
            Primitive::<i32>::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)])
                .to(DataType::Int32),
        );
        let fields = vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ];
        (fields, vec![strings, ints])
    }

    #[test]
    fn basic() {
        let (fields, values) = some_values();

        let array = StructArray::from_data(fields.clone(), values.clone(), None);

        let mut a = GrowableStruct::new(&[&array], false, 0);

        a.extend(0, 1, 2);
        let result: StructArray = a.into();

        let expected = StructArray::from_data(
            fields,
            vec![values[0].slice(1, 2).into(), values[1].slice(1, 2).into()],
            None,
        );
        assert_eq!(result, expected)
    }

    #[test]
    fn offset() {
        let (fields, values) = some_values();

        let array = StructArray::from_data(fields.clone(), values.clone(), None).slice(1, 3);

        let mut a = GrowableStruct::new(&[&array], false, 0);

        a.extend(0, 1, 2);
        let result: StructArray = a.into();

        let expected = StructArray::from_data(
            fields,
            vec![values[0].slice(2, 2).into(), values[1].slice(2, 2).into()],
            None,
        );

        assert_eq!(result, expected);
    }

    #[test]
    fn nulls() {
        let (fields, values) = some_values();

        let array = StructArray::from_data(
            fields.clone(),
            values.clone(),
            Some(Bitmap::from((&[0b00000010], 5))),
        );

        let mut a = GrowableStruct::new(&[&array], false, 0);

        a.extend(0, 1, 2);
        let result: StructArray = a.into();

        let expected = StructArray::from_data(
            fields,
            vec![values[0].slice(1, 2).into(), values[1].slice(1, 2).into()],
            Some(Bitmap::from((&[0b00000010], 5)).slice(1, 2)),
        );

        assert_eq!(result, expected)
    }

    #[test]
    fn many() {
        let (fields, values) = some_values();

        let array = StructArray::from_data(fields.clone(), values.clone(), None);

        let mut mutable = GrowableStruct::new(&[&array, &array], false, 0);

        mutable.extend(0, 1, 2);
        mutable.extend(1, 0, 2);
        let result: StructArray = mutable.into();

        let expected_string: Arc<dyn Array> = Arc::new(Utf8Array::<i32>::from_iter(vec![
            Some("aa"),
            None,
            Some("a"),
            Some("aa"),
        ]));
        let expected_int: Arc<dyn Array> = Arc::new(
            Primitive::<i32>::from(vec![Some(2), Some(3), Some(1), Some(2)]).to(DataType::Int32),
        );

        let expected = StructArray::from_data(fields, vec![expected_string, expected_int], None);
        assert_eq!(result, expected)
    }
}
