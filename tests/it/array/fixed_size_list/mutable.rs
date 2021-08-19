use arrow2::array::*;
use arrow2::datatypes::DataType;

#[test]
fn primitive() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        Some(vec![None, None, None]),
        Some(vec![Some(4), None, Some(6)]),
    ];

    let list: FixedSizeListArray =
        MutableFixedSizeListArray::<MutablePrimitiveArray<i32>>::try_from_iter(
            data,
            3,
            DataType::Int32,
        )
        .unwrap()
        .into();

    let a = list.value(0);
    let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

    let expected = Int32Array::from(vec![Some(1i32), Some(2), Some(3)]);
    assert_eq!(a, &expected);

    let a = list.value(1);
    let a = a.as_any().downcast_ref::<Int32Array>().unwrap();

    let expected = Int32Array::from(vec![None, None, None]);
    assert_eq!(a, &expected)
}
