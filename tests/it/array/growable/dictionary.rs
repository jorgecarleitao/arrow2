use arrow2::array::growable::{Growable, GrowableDictionary};
use arrow2::array::*;
use arrow2::datatypes::DataType;
use arrow2::error::Result;

#[test]
fn test_single() -> Result<()> {
    let original_data = vec![Some("a"), Some("b"), Some("a")];

    let data = original_data.clone();
    let mut array = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    array.try_extend(data)?;
    let array = array.into();

    // same values, less keys
    let expected = DictionaryArray::<i32>::from_data(
        PrimitiveArray::from(vec![Some(1), Some(0)]),
        Box::new(Utf8Array::<i32>::from(&original_data)),
    );

    let mut growable = GrowableDictionary::new(&[&array], false, 0);

    growable.extend(0, 1, 2);

    let result: DictionaryArray<i32> = growable.into();

    assert_eq!(result, expected);
    Ok(())
}

#[test]
fn test_negative_keys() {
    let vals = vec![Some("a"), Some("b"), Some("c")];
    let keys = vec![0, 1, 2, -1];

    let keys = PrimitiveArray::from_data(
        DataType::Int32,
        keys.into(),
        Some(vec![true, true, true, false].into()),
    );

    let arr = DictionaryArray::from_data(keys, Box::new(Utf8Array::<i32>::from(vals)));
    // check that we don't panic with negative keys to usize conversion
    let mut growable = GrowableDictionary::new(&[&arr], false, 0);
    growable.extend(0, 0, 4);
    let out: DictionaryArray<i32> = growable.into();
    assert_eq!(out, arr);
}

#[test]
fn test_multi() -> Result<()> {
    let mut original_data1 = vec![Some("a"), Some("b"), None, Some("a")];
    let original_data2 = vec![Some("c"), Some("b"), None, Some("a")];

    let data1 = original_data1.clone();
    let data2 = original_data2.clone();

    let mut array1 = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    array1.try_extend(data1)?;
    let array1: DictionaryArray<i32> = array1.into();

    let mut array2 = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    array2.try_extend(data2)?;
    let array2: DictionaryArray<i32> = array2.into();

    // same values, less keys
    original_data1.extend(original_data2.iter().cloned());
    let expected = DictionaryArray::<i32>::from_data(
        PrimitiveArray::from(vec![Some(1), None, Some(3), None]),
        Box::new(Utf8Array::<i32>::from_slice(&["a", "b", "c", "b", "a"])),
    );

    let mut growable = GrowableDictionary::new(&[&array1, &array2], false, 0);

    growable.extend(0, 1, 2);
    growable.extend(1, 1, 2);

    let result: DictionaryArray<i32> = growable.into();

    assert_eq!(result, expected);
    Ok(())
}
