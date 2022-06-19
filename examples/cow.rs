// This example demos how to operate on arrays in-place.
use arrow2::array::{Array, PrimitiveArray};
use arrow2::compute::arity_assign;

fn main() {
    // say we have have received an `Array`
    let mut array: Box<dyn Array> = PrimitiveArray::from_vec(vec![1i32, 2]).boxed();

    // we can apply a transformation to its values without allocating a new array as follows:

    // 1. downcast it to the correct type (known via `array.data_type().to_physical_type()`)
    let array = array
        .as_any_mut()
        .downcast_mut::<PrimitiveArray<i32>>()
        .unwrap();

    // 2. call `unary` with the function to apply to each value
    arity_assign::unary(array, |x| x * 10);

    // confirm that it gives the right result :)
    assert_eq!(array, &PrimitiveArray::from_vec(vec![10i32, 20]));
}
