use arrow2::{
    array::{Array, BooleanArray},
    bitmap::Bitmap,
    datatypes::DataType,
};

mod mutable;

#[test]
fn basics() {
    let data = vec![Some(true), None, Some(false)];

    let array: BooleanArray = data.into_iter().collect();

    assert_eq!(array.data_type(), &DataType::Boolean);

    assert!(array.value(0));
    assert!(!array.value(1));
    assert!(!array.value(2));
    assert!(!unsafe { array.value_unchecked(2) });
    assert_eq!(array.values(), &Bitmap::from_u8_slice(&[0b00000001], 3));
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice(&[0b00000101], 3))
    );
    assert!(array.is_valid(0));
    assert!(!array.is_valid(1));
    assert!(array.is_valid(2));

    let array2 = BooleanArray::from_data(
        DataType::Boolean,
        array.values().clone(),
        array.validity().cloned(),
    );
    assert_eq!(array, array2);

    let array = array.slice(1, 2);
    assert!(!array.value(0));
    assert!(!array.value(1));
}

#[test]
fn with_validity() {
    let bitmap = Bitmap::from([true, false, true]);
    let a = BooleanArray::from_data(DataType::Boolean, bitmap, None);
    let a = a.with_validity(Some(Bitmap::from([true, false, true])));
    assert!(a.validity().is_some());
}

#[test]
fn display() {
    let array = BooleanArray::from([Some(true), None, Some(false)]);
    assert_eq!(format!("{:?}", array), "BooleanArray[true, None, false]");
}

#[test]
fn into_mut_valid() {
    let bitmap = Bitmap::from([true, false, true]);
    let a = BooleanArray::from_data(DataType::Boolean, bitmap, None);
    let _ = a.into_mut().right().unwrap();
}

#[test]
fn into_mut_invalid() {
    let bitmap = Bitmap::from([true, false, true]);
    let _other = bitmap.clone(); // values are shared
    let a = BooleanArray::from_data(DataType::Boolean, bitmap, None);
    let _ = a.into_mut().left().unwrap();
}

#[test]
fn empty() {
    let array = BooleanArray::new_empty(DataType::Boolean);
    assert_eq!(array.values().len(), 0);
    assert_eq!(array.validity(), None);
}

#[test]
fn from_trusted_len_iter() {
    let iter = std::iter::repeat(true).take(2).map(Some);
    let a = BooleanArray::from_trusted_len_iter(iter);
    assert_eq!(a.len(), 2);
}

#[test]
fn from_iter() {
    let iter = std::iter::repeat(true).take(2).map(Some);
    let a: BooleanArray = iter.collect();
    assert_eq!(a.len(), 2);
}
