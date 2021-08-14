use arrow2::array::*;
use arrow2::datatypes::{DataType, TimeUnit};
use arrow2::ffi::try_from;
use arrow2::{error::Result, ffi};
use std::sync::Arc;

fn test_release(expected: impl Array + 'static) -> Result<()> {
    // create a `ArrowArray` from the data.
    let b: Arc<dyn Array> = Arc::new(expected);

    // export the array as 2 pointers.
    let _ = ffi::export_to_c(b)?;

    Ok(())
}

fn test_round_trip(expected: impl Array + Clone + 'static) -> Result<()> {
    let b: Arc<dyn Array> = Arc::new(expected.clone());
    let expected = Box::new(expected) as Box<dyn Array>;

    // create a `ArrowArray` from the data.
    let array = Arc::new(ffi::export_to_c(b)?);

    let (_, _) = array.references();

    let result = try_from(array)?;

    assert_eq!(&result, &expected);
    Ok(())
}

#[test]
fn test_u32() -> Result<()> {
    let data = Int32Array::from(&[Some(2), None, Some(1), None]);
    test_release(data)
}

#[test]
fn test_u64() -> Result<()> {
    let data = UInt64Array::from(&[Some(2), None, Some(1), None]);
    test_round_trip(data)
}

#[test]
fn test_i64() -> Result<()> {
    let data = Int64Array::from(&[Some(2), None, Some(1), None]);
    test_round_trip(data)
}

#[test]
fn test_utf8() -> Result<()> {
    let data = Utf8Array::<i32>::from(&vec![Some("a"), None, Some("bb"), None]);
    test_round_trip(data)
}

#[test]
fn test_large_utf8() -> Result<()> {
    let data = Utf8Array::<i64>::from(&vec![Some("a"), None, Some("bb"), None]);
    test_round_trip(data)
}

#[test]
fn test_binary() -> Result<()> {
    let data =
        BinaryArray::<i32>::from(&vec![Some(b"a".as_ref()), None, Some(b"bb".as_ref()), None]);
    test_round_trip(data)
}

#[test]
fn test_timestamp_tz() -> Result<()> {
    let data = Int64Array::from(&vec![Some(2), None, None]).to(DataType::Timestamp(
        TimeUnit::Second,
        Some("UTC".to_string()),
    ));
    test_round_trip(data)
}

#[test]
fn test_large_binary() -> Result<()> {
    let data =
        BinaryArray::<i64>::from(&vec![Some(b"a".as_ref()), None, Some(b"bb".as_ref()), None]);
    test_round_trip(data)
}

#[test]
fn test_list() -> Result<()> {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data)?;

    let array: ListArray<i32> = array.into();

    test_round_trip(array)
}

#[test]
fn test_list_list() -> Result<()> {
    let data = vec![
        Some(vec![
            Some(vec![None]),
            Some(vec![Some(2)]),
            Some(vec![Some(3)]),
        ]),
        None,
        Some(vec![Some(vec![Some(4), None, Some(6)])]),
    ];

    let mut array =
        MutableListArray::<i32, MutableListArray<i32, MutablePrimitiveArray<i32>>>::new();
    array.try_extend(data)?;

    let array: ListArray<i32> = array.into();

    test_round_trip(array)
}

#[test]
fn test_dict() -> Result<()> {
    let data = vec![Some("a"), Some("a"), None, Some("b")];

    let mut array = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    array.try_extend(data)?;

    let array: DictionaryArray<i32> = array.into();

    test_round_trip(array)
}
