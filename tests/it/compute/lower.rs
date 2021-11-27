use arrow2::{array::*, compute::lower::*, error::Result};

fn with_nulls_utf8<O: Offset>() -> Result<()> {
    let cases = vec![
        // identity
        (
            vec![Some("hello"), None, Some("world")],
            vec![Some("hello"), None, Some("world")],
        ),
        // part of input
        (
            vec![Some("Hello"), None, Some("wOrld")],
            vec![Some("hello"), None, Some("world")],
        ),
        // all input
        (
            vec![Some("HELLO"), None, Some("WORLD")],
            vec![Some("hello"), None, Some("world")],
        ),
    ];

    cases
        .into_iter()
        .try_for_each::<_, Result<()>>(|(array, expected)| {
            let array = Utf8Array::<O>::from(&array);
            let result = lower(&array)?;
            assert_eq!(array.len(), result.len());

            let result = result.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
            let expected = Utf8Array::<O>::from(&expected);

            assert_eq!(&expected, result);
            Ok(())
        })?;

    Ok(())
}

#[test]
fn with_nulls_string() -> Result<()> {
    with_nulls_utf8::<i32>()
}

#[test]
fn with_nulls_large_string() -> Result<()> {
    with_nulls_utf8::<i64>()
}

fn without_nulls_utf8<O: Offset>() -> Result<()> {
    let cases = vec![
        // identity
        (vec!["hello", "world"], vec!["hello", "world"]),
        // part of input
        (vec!["Hello", "wOrld"], vec!["hello", "world"]),
        // all input
        (vec!["HELLO", "WORLD"], vec!["hello", "world"]),
    ];

    cases
        .into_iter()
        .try_for_each::<_, Result<()>>(|(array, expected)| {
            let array = Utf8Array::<O>::from_slice(&array);
            let result = lower(&array)?;
            assert_eq!(array.len(), result.len());

            let result = result.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
            let expected = Utf8Array::<O>::from_slice(&expected);
            assert_eq!(&expected, result);
            Ok(())
        })?;

    Ok(())
}

#[test]
fn without_nulls_string() -> Result<()> {
    without_nulls_utf8::<i32>()
}

#[test]
fn without_nulls_large_string() -> Result<()> {
    without_nulls_utf8::<i64>()
}

fn with_null_binarys<O: Offset>() -> Result<()> {
    let cases = vec![
        // identity
        (
            vec![Some(b"hello"), None, Some(b"world")],
            vec![Some(b"hello"), None, Some(b"world")],
        ),
        // part of input
        (
            vec![Some(b"Hello"), None, Some(b"wOrld")],
            vec![Some(b"Hello"), None, Some(b"wOrld")],
        ),
        // all input
        (
            vec![Some(b"HELLO"), None, Some(b"WORLD")],
            vec![Some(b"HELLO"), None, Some(b"WORLD")],
        ),
    ];

    cases
        .into_iter()
        .try_for_each::<_, Result<()>>(|(array, expected)| {
            let array = BinaryArray::<O>::from(&array);
            let result = lower(&array)?;
            assert_eq!(array.len(), result.len());

            let result = result.as_any().downcast_ref::<BinaryArray<O>>().unwrap();
            let expected = BinaryArray::<O>::from(&expected);

            assert_eq!(&expected, result);
            Ok(())
        })?;

    Ok(())
}

#[test]
fn with_nulls_binary() -> Result<()> {
    with_null_binarys::<i32>()
}

#[test]
fn with_nulls_large_binary() -> Result<()> {
    with_null_binarys::<i64>()
}

fn without_null_binarys<O: Offset>() -> Result<()> {
    let cases = vec![
        // identity
        (vec![b"hello", b"world"], vec![b"hello", b"world"]),
        // part of input
        (vec![b"Hello", b"wOrld"], vec![b"Hello", b"wOrld"]),
        // all input
        (vec![b"HELLO", b"WORLD"], vec![b"HELLO", b"WORLD"]),
    ];

    cases
        .into_iter()
        .try_for_each::<_, Result<()>>(|(array, expected)| {
            let array = BinaryArray::<O>::from_slice(&array);
            let result = lower(&array)?;
            assert_eq!(array.len(), result.len());

            let result = result.as_any().downcast_ref::<BinaryArray<O>>().unwrap();
            let expected = BinaryArray::<O>::from_slice(&expected);

            assert_eq!(&expected, result);
            Ok(())
        })?;

    Ok(())
}

#[test]
fn without_nulls_binary() -> Result<()> {
    without_null_binarys::<i32>()
}

#[test]
fn without_nulls_large_binary() -> Result<()> {
    without_null_binarys::<i64>()
}

#[test]
fn consistency() {
    use arrow2::datatypes::DataType::*;
    use arrow2::datatypes::TimeUnit;
    let datatypes = vec![
        Null,
        Boolean,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Int8,
        Int16,
        Int32,
        Int64,
        Float32,
        Float64,
        Timestamp(TimeUnit::Second, None),
        Timestamp(TimeUnit::Millisecond, None),
        Timestamp(TimeUnit::Microsecond, None),
        Timestamp(TimeUnit::Nanosecond, None),
        Time64(TimeUnit::Microsecond),
        Time64(TimeUnit::Nanosecond),
        Date32,
        Time32(TimeUnit::Second),
        Time32(TimeUnit::Millisecond),
        Date64,
        Utf8,
        LargeUtf8,
        Binary,
        LargeBinary,
        Duration(TimeUnit::Second),
        Duration(TimeUnit::Millisecond),
        Duration(TimeUnit::Microsecond),
        Duration(TimeUnit::Nanosecond),
    ];

    datatypes.into_iter().for_each(|d1| {
        let array = new_null_array(d1.clone(), 10);
        if can_lower(&d1) {
            assert!(lower(array.as_ref()).is_ok());
        } else {
            assert!(lower(array.as_ref()).is_err());
        }
    });
}
