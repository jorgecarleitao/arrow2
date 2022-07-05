mod mutable;

use arrow2::{array::*, datatypes::DataType};

#[test]
fn try_new_ok() {
    let values = Utf8Array::<i32>::from_slice(&["a", "aa"]);
    let data_type =
        DataType::Dictionary(i32::KEY_TYPE, Box::new(values.data_type().clone()), false);
    let array = DictionaryArray::try_new(
        data_type,
        PrimitiveArray::from_vec(vec![1, 0]),
        values.boxed(),
    )
    .unwrap();

    assert_eq!(array.keys(), &PrimitiveArray::from_vec(vec![1i32, 0]));
    assert_eq!(
        &Utf8Array::<i32>::from_slice(&["a", "aa"]) as &dyn Array,
        array.values().as_ref(),
    );
    assert!(!array.is_ordered());

    assert_eq!(format!("{:?}", array), "DictionaryArray[aa, a]");
}

#[test]
fn try_new_incorrect_key() {
    let values = Utf8Array::<i32>::from_slice(&["a", "aa"]);
    let data_type =
        DataType::Dictionary(i16::KEY_TYPE, Box::new(values.data_type().clone()), false);

    let r = DictionaryArray::try_new(
        data_type,
        PrimitiveArray::from_vec(vec![1, 0]),
        values.boxed(),
    )
    .is_err();

    assert!(r);
}

#[test]
fn try_new_incorrect_dt() {
    let values = Utf8Array::<i32>::from_slice(&["a", "aa"]);
    let data_type = DataType::Int32;

    let r = DictionaryArray::try_new(
        data_type,
        PrimitiveArray::from_vec(vec![1, 0]),
        values.boxed(),
    )
    .is_err();

    assert!(r);
}

#[test]
fn try_new_incorrect_values_dt() {
    let values = Utf8Array::<i32>::from_slice(&["a", "aa"]);
    let data_type = DataType::Dictionary(i32::KEY_TYPE, Box::new(DataType::LargeUtf8), false);

    let r = DictionaryArray::try_new(
        data_type,
        PrimitiveArray::from_vec(vec![1, 0]),
        values.boxed(),
    )
    .is_err();

    assert!(r);
}

#[test]
fn try_new_out_of_bounds() {
    let values = Utf8Array::<i32>::from_slice(&["a", "aa"]);

    let r = DictionaryArray::try_from_keys(PrimitiveArray::from_vec(vec![2, 0]), values.boxed())
        .is_err();

    assert!(r);
}

#[test]
fn try_new_out_of_bounds_neg() {
    let values = Utf8Array::<i32>::from_slice(&["a", "aa"]);

    let r = DictionaryArray::try_from_keys(PrimitiveArray::from_vec(vec![-1, 0]), values.boxed())
        .is_err();

    assert!(r);
}

#[test]
fn new_null() {
    let dt = DataType::Dictionary(i16::KEY_TYPE, Box::new(DataType::Int32), false);
    let array = DictionaryArray::<i16>::new_null(dt, 2);

    assert_eq!(format!("{:?}", array), "DictionaryArray[None, None]");
}

#[test]
fn new_empty() {
    let dt = DataType::Dictionary(i16::KEY_TYPE, Box::new(DataType::Int32), false);
    let array = DictionaryArray::<i16>::new_empty(dt);

    assert_eq!(format!("{:?}", array), "DictionaryArray[]");
}

#[test]
fn with_validity() {
    let values = Utf8Array::<i32>::from_slice(&["a", "aa"]);
    let array =
        DictionaryArray::try_from_keys(PrimitiveArray::from_vec(vec![1, 0]), values.boxed())
            .unwrap();

    let array = array.with_validity(Some([true, false].into()));

    assert_eq!(format!("{:?}", array), "DictionaryArray[aa, None]");
}

#[test]
fn rev_iter() {
    let values = Utf8Array::<i32>::from_slice(&["a", "aa"]);
    let array =
        DictionaryArray::try_from_keys(PrimitiveArray::from_vec(vec![1, 0]), values.boxed())
            .unwrap();

    let mut iter = array.into_iter();
    assert_eq!(iter.by_ref().rev().count(), 2);
    assert_eq!(iter.size_hint(), (0, Some(0)));
}

#[test]
fn iter_values() {
    let values = Utf8Array::<i32>::from_slice(&["a", "aa"]);
    let array =
        DictionaryArray::try_from_keys(PrimitiveArray::from_vec(vec![1, 0]), values.boxed())
            .unwrap();

    let mut iter = array.values_iter();
    assert_eq!(iter.by_ref().count(), 2);
    assert_eq!(iter.size_hint(), (0, Some(0)));
}

#[test]
fn keys_values_iter() {
    let values = Utf8Array::<i32>::from_slice(&["a", "aa"]);
    let array =
        DictionaryArray::try_from_keys(PrimitiveArray::from_vec(vec![1, 0]), values.boxed())
            .unwrap();

    assert_eq!(array.keys_values_iter().collect::<Vec<_>>(), vec![1, 0]);
}
