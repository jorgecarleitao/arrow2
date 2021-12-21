use std::sync::Arc;

use arrow2::array::*;
use arrow2::buffer::Buffer;
use arrow2::datatypes::DataType;

mod mutable;

#[test]
fn display() {
    let values = Buffer::from_slice([1, 2, 3, 4, 5]);
    let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let array = ListArray::<i32>::from_data(
        data_type,
        Buffer::from_slice([0, 2, 2, 3, 5]),
        Arc::new(values),
        None,
    );

    assert_eq!(
        format!("{:?}", array),
        "ListArray[\nInt32[1, 2],\nInt32[],\nInt32[3],\nInt32[4, 5]\n]"
    );
}

#[test]
#[should_panic(expected = "The child's datatype must match the inner type of the \'data_type\'")]
fn test_nested_panic() {
    let values = Buffer::from_slice([1, 2, 3, 4, 5]);
    let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let array = ListArray::<i32>::from_data(
        data_type.clone(),
        Buffer::from_slice([0, 2, 2, 3, 5]),
        Arc::new(values),
        None,
    );

    // The datatype for the nested array has to be created considering
    // the nested structure of the child data
    let _ = ListArray::<i32>::from_data(
        data_type,
        Buffer::from_slice([0, 2, 4]),
        Arc::new(array),
        None,
    );
}

#[test]
fn test_nested_display() {
    let values = Buffer::from_slice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let values = PrimitiveArray::<i32>::from_data(DataType::Int32, values, None);

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let array = ListArray::<i32>::from_data(
        data_type,
        Buffer::from_slice([0, 2, 4, 7, 7, 8, 10]),
        Arc::new(values),
        None,
    );

    let data_type = ListArray::<i32>::default_datatype(array.data_type().clone());
    let nested = ListArray::<i32>::from_data(
        data_type,
        Buffer::from_slice([0, 2, 5, 6]),
        Arc::new(array),
        None,
    );

    let expected = "ListArray[\nListArray[\nInt32[1, 2],\nInt32[3, 4]\n],\nListArray[\nInt32[5, 6, 7],\nInt32[],\nInt32[8]\n],\nListArray[\nInt32[9, 10]\n]\n]";
    assert_eq!(format!("{:?}", nested), expected);
}
