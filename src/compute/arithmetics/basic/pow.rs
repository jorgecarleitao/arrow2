use num::traits::Pow;

use crate::{
    array::{Array, PrimitiveArray},
    compute::arity::unary,
    types::NativeType,
};

#[inline]
pub fn powf_scalar<T>(array: &PrimitiveArray<T>, exponent: T) -> PrimitiveArray<T>
where
    T: NativeType + Pow<T, Output = T>,
{
    unary(array, |x| x.pow(exponent), array.data_type())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Primitive;
    use crate::datatypes::DataType;

    #[test]
    fn test_raise_power_scalar() {
        let a = Primitive::from(&vec![Some(2f32), None]).to(DataType::Float32);
        let actual = powf_scalar(&a, 2.0);
        let expected = Primitive::from(&vec![Some(4f32), None]).to(DataType::Float32);
        assert_eq!(expected, actual);
    }
}
