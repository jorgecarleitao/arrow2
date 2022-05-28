use std::sync::Arc;

use arrow2::array::*;
use arrow2::buffer::Buffer;
use arrow2::datatypes::DataType;

mod mutable;

#[test]
fn debug() {
    let values = Buffer::from(vec![1, 2, 3, 4, 5]);
    let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let array = ListArray::<i32>::from_data(
        data_type,
        Buffer::from(vec![0, 2, 2, 3, 5]),
        Arc::new(values),
        None,
    );

    assert_eq!(format!("{:?}", array), "ListArray[[1, 2], [], [3], [4, 5]]");
}

#[test]
#[should_panic]
fn test_nested_panic() {
    let values = Buffer::from(vec![1, 2, 3, 4, 5]);
    let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let array = ListArray::<i32>::from_data(
        data_type.clone(),
        Buffer::from(vec![0, 2, 2, 3, 5]),
        Arc::new(values),
        None,
    );

    // The datatype for the nested array has to be created considering
    // the nested structure of the child data
    let _ = ListArray::<i32>::from_data(
        data_type,
        Buffer::from(vec![0, 2, 4]),
        Arc::new(array),
        None,
    );
}

#[test]
fn test_nested_display() {
    let values = Buffer::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let array = ListArray::<i32>::from_data(
        data_type,
        Buffer::from(vec![0, 2, 4, 7, 7, 8, 10]),
        Arc::new(values),
        None,
    );

    let data_type = ListArray::<i32>::default_datatype(array.data_type().clone());
    let nested = ListArray::<i32>::from_data(
        data_type,
        Buffer::from(vec![0, 2, 5, 6]),
        Arc::new(array),
        None,
    );

    let expected = "ListArray[[[1, 2], [3, 4]], [[5, 6, 7], [], [8]], [[9, 10]]]";
    assert_eq!(format!("{:?}", nested), expected);
}
