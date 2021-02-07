use std::convert::TryFrom;

use crate::{
    buffer::{types::NativeType, Bitmap, Buffer},
    datatypes::{DataType, Field},
};

use super::{specification::check_offsets, Array};

pub unsafe trait Offset: NativeType {
    fn is_large() -> bool;

    fn to_usize(&self) -> Option<usize>;
}

unsafe impl Offset for i32 {
    #[inline]
    fn is_large() -> bool {
        false
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        Some(*self as usize)
    }
}

unsafe impl Offset for i64 {
    #[inline]
    fn is_large() -> bool {
        true
    }

    #[inline]
    fn to_usize(&self) -> Option<usize> {
        usize::try_from(*self).ok()
    }
}

#[derive(Debug)]
pub struct ListArray<O: Offset> {
    data_type: DataType,
    offsets: Buffer<O>,
    values: Box<dyn Array>,
    validity: Option<Bitmap>,
}

impl<O: Offset> ListArray<O> {
    pub fn from_data(
        offsets: Buffer<O>,
        values: Box<dyn Array>,
        validity: Option<Bitmap>,
        field_options: Option<(&str, bool)>,
    ) -> Self {
        check_offsets(&offsets, values.len());

        let (field_name, field_nullable) = field_options.unwrap_or(("item", true));

        let field = Box::new(Field::new(
            field_name,
            values.data_type().clone(),
            field_nullable,
        ));

        let data_type = if O::is_large() {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        };

        Self {
            data_type,
            offsets,
            values,
            validity,
        }
    }
}

impl<O: Offset> Array for ListArray<O> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    fn nulls(&self) -> &Option<Bitmap> {
        &self.validity
    }
}

#[cfg(test)]
mod tests {
    use crate::array::primitive::PrimitiveArray;

    use super::*;

    #[test]
    fn test_create() {
        let values = Buffer::from([1, 2, 3, 4, 5]);
        let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

        ListArray::<i32>::from_data(Buffer::from([0, 2, 2, 3, 5]), Box::new(values), None, None);
    }
}
