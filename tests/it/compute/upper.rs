use arrow2::{array::*, compute::upper::*, error::Result};

fn with_nulls_utf8<O: Offset>() -> Result<()> {
    let cases = vec![
        // identity
        (
            vec![Some("hello"), None, Some("world")],
            vec![Some("HELLO"), None, Some("WORLD")],
        ),
        // part of input
        (
            vec![Some("Hello"), None, Some("wOrld")],
            vec![Some("HELLO"), None, Some("WORLD")],
        ),
        // all input
        (
            vec![Some("hello"), None, Some("world")],
            vec![Some("HELLO"), None, Some("WORLD")],
        ),
        // UTF8 characters
        (
            vec![
                None,
                Some("السلام عليكم"),
                Some("Dobrý den"),
                Some("שָׁלוֹם"),
                Some("नमस्ते"),
                Some("こんにちは"),
                Some("안녕하세요"),
                Some("你好"),
                Some("Olá"),
                Some("Здравствуйте"),
                Some("Hola"),
            ],
            vec![
                None,
                Some("السلام عليكم"),
                Some("DOBRÝ DEN"),
                Some("שָׁלוֹם"),
                Some("नमस्ते"),
                Some("こんにちは"),
                Some("안녕하세요"),
                Some("你好"),
                Some("OLÁ"),
                Some("ЗДРАВСТВУЙТЕ"),
                Some("HOLA"),
            ],
        ),
    ];

    cases
        .into_iter()
        .try_for_each::<_, Result<()>>(|(array, expected)| {
            let array = Utf8Array::<O>::from(&array);
            let result = upper(&array)?;
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
        (vec!["hello", "world"], vec!["HELLO", "WORLD"]),
        // part of input
        (vec!["Hello", "wOrld"], vec!["HELLO", "WORLD"]),
        // all input
        (vec!["HELLO", "WORLD"], vec!["HELLO", "WORLD"]),
        // UTF8 characters
        (
            vec![
                "السلام عليكم",
                "Dobrý den",
                "שָׁלוֹם",
                "नमस्ते",
                "こんにちは",
                "안녕하세요",
                "你好",
                "Olá",
                "Здравствуйте",
                "Hola",
            ],
            vec![
                "السلام عليكم",
                "DOBRÝ DEN",
                "שָׁלוֹם",
                "नमस्ते",
                "こんにちは",
                "안녕하세요",
                "你好",
                "OLÁ",
                "ЗДРАВСТВУЙТЕ",
                "HOLA",
            ],
        ),
    ];

    cases
        .into_iter()
        .try_for_each::<_, Result<()>>(|(array, expected)| {
            let array = Utf8Array::<O>::from_slice(&array);
            let result = upper(&array)?;
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
        if can_upper(&d1) {
            assert!(upper(array.as_ref()).is_ok());
        } else {
            assert!(upper(array.as_ref()).is_err());
        }
    });
}
