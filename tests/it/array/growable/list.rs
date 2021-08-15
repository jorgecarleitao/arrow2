use arrow2::array::{
    growable::{Growable, GrowableList},
    ListArray, MutableListArray, MutablePrimitiveArray, TryExtend,
};

fn create_list_array(data: Vec<Option<Vec<Option<i32>>>>) -> ListArray<i32> {
    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data).unwrap();
    array.into()
}

#[test]
fn basic() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        Some(vec![Some(4), Some(5)]),
        Some(vec![Some(6i32), Some(7), Some(8)]),
    ];

    let array = create_list_array(data);

    let mut a = GrowableList::new(vec![&array], false, 0);
    a.extend(0, 0, 1);

    let result: ListArray<i32> = a.into();

    let expected = vec![Some(vec![Some(1i32), Some(2), Some(3)])];
    let expected = create_list_array(expected);

    assert_eq!(result, expected)
}

#[test]
fn null_offset() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(6i32), Some(7), Some(8)]),
    ];
    let array = create_list_array(data);
    let array = array.slice(1, 2);

    let mut a = GrowableList::new(vec![&array], false, 0);
    a.extend(0, 1, 1);

    let result: ListArray<i32> = a.into();

    let expected = vec![Some(vec![Some(6i32), Some(7), Some(8)])];
    let expected = create_list_array(expected);

    assert_eq!(result, expected)
}

#[test]
fn null_offsets() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(6i32), None, Some(8)]),
    ];
    let array = create_list_array(data);
    let array = array.slice(1, 2);

    let mut a = GrowableList::new(vec![&array], false, 0);
    a.extend(0, 1, 1);

    let result: ListArray<i32> = a.into();

    let expected = vec![Some(vec![Some(6i32), None, Some(8)])];
    let expected = create_list_array(expected);

    assert_eq!(result, expected)
}

#[test]
fn test_from_two_lists() {
    let data_1 = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(6i32), None, Some(8)]),
    ];
    let array_1 = create_list_array(data_1);

    let data_2 = vec![
        Some(vec![Some(8i32), Some(7), Some(6)]),
        Some(vec![Some(5i32), None, Some(4)]),
        Some(vec![Some(2i32), Some(1), Some(0)]),
    ];
    let array_2 = create_list_array(data_2);

    let mut a = GrowableList::new(vec![&array_1, &array_2], false, 6);
    a.extend(0, 0, 2);
    a.extend(1, 1, 1);

    let result: ListArray<i32> = a.into();

    let expected = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(5i32), None, Some(4)]),
    ];
    let expected = create_list_array(expected);

    assert_eq!(result, expected);
}
