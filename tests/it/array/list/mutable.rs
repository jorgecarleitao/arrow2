use std::sync::Arc;

use arrow2::{array::*, bitmap::Bitmap, buffer::Buffer, datatypes::DataType};

#[test]
fn basics() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data).unwrap();
    let array: ListArray<i32> = array.into();

    let values = PrimitiveArray::<i32>::from_data(
        DataType::Int32,
        Buffer::from([1, 2, 3, 4, 0, 6]),
        Some(Bitmap::from([true, true, true, true, false, true])),
    );

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let expected = ListArray::<i32>::from_data(
        data_type,
        Buffer::from([0, 3, 3, 6]),
        Arc::new(values),
        Some(Bitmap::from([true, false, true])),
    );
    assert_eq!(expected, array);
}
