use arrow2::array::*;
use arrow2::compute::cast::{can_cast_types, cast, CastOptions};
use arrow2::datatypes::*;
use arrow2::types::NativeType;

#[test]
fn i32_to_f64() {
    let array = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
    let b = cast(&array, &DataType::Float64, CastOptions::default()).unwrap();
    let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((5.0 - c.value(0)).abs() < f64::EPSILON);
    assert!((6.0 - c.value(1)).abs() < f64::EPSILON);
    assert!((7.0 - c.value(2)).abs() < f64::EPSILON);
    assert!((8.0 - c.value(3)).abs() < f64::EPSILON);
    assert!((9.0 - c.value(4)).abs() < f64::EPSILON);
}

#[test]
fn i32_as_f64_no_overflow() {
    let array = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
    let b = cast(
        &array,
        &DataType::Float64,
        CastOptions {
            wrapped: true,
            ..Default::default()
        },
    )
    .unwrap();
    let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((5.0 - c.value(0)).abs() < f64::EPSILON);
    assert!((6.0 - c.value(1)).abs() < f64::EPSILON);
    assert!((7.0 - c.value(2)).abs() < f64::EPSILON);
    assert!((8.0 - c.value(3)).abs() < f64::EPSILON);
    assert!((9.0 - c.value(4)).abs() < f64::EPSILON);
}

#[test]
fn u16_as_u8_overflow() {
    let array = UInt16Array::from_slice(&[255, 256, 257, 258, 259]);
    let b = cast(
        &array,
        &DataType::UInt8,
        CastOptions {
            wrapped: true,
            ..Default::default()
        },
    )
    .unwrap();
    let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
    let values = c.values().as_slice();

    assert_eq!(values, &[255, 0, 1, 2, 3])
}

#[test]
fn u16_as_u8_no_overflow() {
    let array = UInt16Array::from_slice(&[1, 2, 3, 4, 5]);
    let b = cast(
        &array,
        &DataType::UInt8,
        CastOptions {
            wrapped: true,
            ..Default::default()
        },
    )
    .unwrap();
    let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
    let values = c.values().as_slice();
    assert_eq!(values, &[1, 2, 3, 4, 5])
}

#[test]
fn f32_as_u8_overflow() {
    let array = Float32Array::from_slice(&[1.1, 5000.0]);
    let b = cast(&array, &DataType::UInt8, CastOptions::default()).unwrap();
    let expected = UInt8Array::from(&[Some(1), None]);
    assert_eq!(expected, b.as_ref());

    let b = cast(
        &array,
        &DataType::UInt8,
        CastOptions {
            wrapped: true,
            ..Default::default()
        },
    )
    .unwrap();
    let expected = UInt8Array::from(&[Some(1), Some(255)]);
    assert_eq!(expected, b.as_ref());
}

#[test]
fn i32_to_u8() {
    let array = Int32Array::from_slice(&[-5, 6, -7, 8, 100000000]);
    let b = cast(&array, &DataType::UInt8, CastOptions::default()).unwrap();
    let expected = UInt8Array::from(&[None, Some(6), None, Some(8), None]);
    let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
    assert_eq!(c, &expected);
}

#[test]
fn i32_to_u8_sliced() {
    let array = Int32Array::from_slice(&[-5, 6, -7, 8, 100000000]);
    let array = array.slice(2, 3);
    let b = cast(&array, &DataType::UInt8, CastOptions::default()).unwrap();
    let expected = UInt8Array::from(&[None, Some(8), None]);
    let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
    assert_eq!(c, &expected);
}

#[test]
fn i32_to_i32() {
    let array = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
    let b = cast(&array, &DataType::Int32, CastOptions::default()).unwrap();
    let c = b.as_any().downcast_ref::<Int32Array>().unwrap();

    let expected = &[5, 6, 7, 8, 9];
    let expected = Int32Array::from_slice(expected);
    assert_eq!(c, &expected);
}

#[test]
fn i32_to_list_i32() {
    let array = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
    let b = cast(
        &array,
        &DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
        CastOptions::default(),
    )
    .unwrap();

    let arr = b.as_any().downcast_ref::<ListArray<i32>>().unwrap();
    assert_eq!(&[0, 1, 2, 3, 4, 5], arr.offsets().as_slice());
    let values = arr.values();
    let c = values
        .as_any()
        .downcast_ref::<PrimitiveArray<i32>>()
        .unwrap();

    let expected = Int32Array::from_slice(&[5, 6, 7, 8, 9]);
    assert_eq!(c, &expected);
}

#[test]
fn i32_to_list_i32_nullable() {
    let input = [Some(5), None, Some(7), Some(8), Some(9)];

    let array = Int32Array::from(input);
    let b = cast(
        &array,
        &DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
        CastOptions::default(),
    )
    .unwrap();

    let arr = b.as_any().downcast_ref::<ListArray<i32>>().unwrap();
    assert_eq!(&[0, 1, 2, 3, 4, 5], arr.offsets().as_slice());
    let values = arr.values();
    let c = values.as_any().downcast_ref::<Int32Array>().unwrap();

    let expected = &[Some(5), None, Some(7), Some(8), Some(9)];
    let expected = Int32Array::from(expected);
    assert_eq!(c, &expected);
}

#[test]
fn i32_to_list_f64_nullable_sliced() {
    let input = [Some(5), None, Some(7), Some(8), None, Some(10)];

    let array = Int32Array::from(input);

    let array = array.slice(2, 4);
    let b = cast(
        &array,
        &DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
        CastOptions::default(),
    )
    .unwrap();

    let arr = b.as_any().downcast_ref::<ListArray<i32>>().unwrap();
    assert_eq!(&[0, 1, 2, 3, 4], arr.offsets().as_slice());
    let values = arr.values();
    let c = values.as_any().downcast_ref::<Float64Array>().unwrap();

    let expected = &[Some(7.0), Some(8.0), None, Some(10.0)];
    let expected = Float64Array::from(expected);
    assert_eq!(c, &expected);
}

#[test]
fn i32_to_binary() {
    let array = Int32Array::from_slice(&[5, 6, 7]);
    let b = cast(&array, &DataType::Binary, CastOptions::default()).unwrap();
    let expected = BinaryArray::<i32>::from(&[Some(b"5"), Some(b"6"), Some(b"7")]);
    let c = b.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
    assert_eq!(c, &expected);
}

#[test]
fn binary_to_i32() {
    let array = BinaryArray::<i32>::from_slice(&["5", "6", "seven", "8", "9.1"]);
    let b = cast(&array, &DataType::Int32, CastOptions::default()).unwrap();
    let c = b.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

    let expected = &[Some(5), Some(6), None, Some(8), None];
    let expected = Int32Array::from(expected);
    assert_eq!(c, &expected);
}

#[test]
fn binary_to_i32_partial() {
    let array = BinaryArray::<i32>::from_slice(&["5", "6", "123 abseven", "aaa", "9.1"]);
    let b = cast(
        &array,
        &DataType::Int32,
        CastOptions {
            partial: true,
            ..Default::default()
        },
    )
    .unwrap();
    let c = b.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

    let expected = &[Some(5), Some(6), Some(123), Some(0), Some(9)];
    let expected = Int32Array::from(expected);
    assert_eq!(c, &expected);
}

#[test]
fn utf8_to_i32() {
    let array = Utf8Array::<i32>::from_slice(&["5", "6", "seven", "8", "9.1"]);
    let b = cast(&array, &DataType::Int32, CastOptions::default()).unwrap();
    let c = b.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

    let expected = &[Some(5), Some(6), None, Some(8), None];
    let expected = Int32Array::from(expected);
    assert_eq!(c, &expected);
}

#[test]
fn utf8_to_i32_partial() {
    let array = Utf8Array::<i32>::from_slice(&["5", "6", "seven", "8aa", "9.1aa"]);
    let b = cast(
        &array,
        &DataType::Int32,
        CastOptions {
            partial: true,
            ..Default::default()
        },
    )
    .unwrap();
    let c = b.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

    let expected = &[Some(5), Some(6), Some(0), Some(8), Some(9)];
    let expected = Int32Array::from(expected);
    assert_eq!(c, &expected);
}

#[test]
fn bool_to_i32() {
    let array = BooleanArray::from(vec![Some(true), Some(false), None]);
    let b = cast(&array, &DataType::Int32, CastOptions::default()).unwrap();
    let c = b.as_any().downcast_ref::<Int32Array>().unwrap();

    let expected = &[Some(1), Some(0), None];
    let expected = Int32Array::from(expected);
    assert_eq!(c, &expected);
}

#[test]
fn bool_to_f64() {
    let array = BooleanArray::from(vec![Some(true), Some(false), None]);
    let b = cast(&array, &DataType::Float64, CastOptions::default()).unwrap();
    let c = b.as_any().downcast_ref::<Float64Array>().unwrap();

    let expected = &[Some(1.0), Some(0.0), None];
    let expected = Float64Array::from(expected);
    assert_eq!(c, &expected);
}

#[test]
fn bool_to_utf8() {
    let array = BooleanArray::from(vec![Some(true), Some(false), None]);
    let b = cast(&array, &DataType::Utf8, CastOptions::default()).unwrap();
    let c = b.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

    let expected = Utf8Array::<i32>::from(&[Some("1"), Some("0"), Some("0")]);
    assert_eq!(c, &expected);
}

#[test]
fn bool_to_binary() {
    let array = BooleanArray::from(vec![Some(true), Some(false), None]);
    let b = cast(&array, &DataType::Binary, CastOptions::default()).unwrap();
    let c = b.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();

    let expected = BinaryArray::<i32>::from(&[Some("1"), Some("0"), Some("0")]);
    assert_eq!(c, &expected);
}

#[test]
fn int32_to_timestamp() {
    let array = Int32Array::from(&[Some(2), Some(10), None]);
    assert!(cast(
        &array,
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        CastOptions::default()
    )
    .is_err());
}

#[test]
fn consistency() {
    use DataType::*;
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
        Timestamp(TimeUnit::Millisecond, Some("+01:00".to_string())),
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
        List(Box::new(Field::new("a", Utf8, true))),
        LargeList(Box::new(Field::new("a", Utf8, true))),
    ];
    for d1 in &datatypes {
        for d2 in &datatypes {
            let array = new_null_array(d1.clone(), 10);
            if can_cast_types(d1, d2) {
                let result = cast(array.as_ref(), d2, CastOptions::default());
                if let Ok(result) = result {
                    assert_eq!(result.data_type(), d2, "type not equal: {:?} {:?}", d1, d2);
                } else {
                    panic!("Cast should have not failed {:?} {:?}", d1, d2);
                }
            } else if cast(array.as_ref(), d2, CastOptions::default()).is_ok() {
                panic!("Cast should have failed {:?} {:?}", d1, d2);
            }
        }
    }
}

fn test_primitive_to_primitive<I: NativeType, O: NativeType>(
    lhs: &[I],
    lhs_type: DataType,
    expected: &[O],
    expected_type: DataType,
) {
    let a = PrimitiveArray::<I>::from_slice(lhs).to(lhs_type);
    let b = cast(&a, &expected_type, CastOptions::default()).unwrap();
    let b = b.as_any().downcast_ref::<PrimitiveArray<O>>().unwrap();
    let expected = PrimitiveArray::<O>::from_slice(expected).to(expected_type);
    assert_eq!(b, &expected);
}

#[test]
fn date32_to_date64() {
    test_primitive_to_primitive(
        &[10000i32, 17890],
        DataType::Date32,
        &[864000000000i64, 1545696000000],
        DataType::Date64,
    );
}

#[test]
fn date64_to_date32() {
    test_primitive_to_primitive(
        &[864000000005i64, 1545696000001],
        DataType::Date64,
        &[10000i32, 17890],
        DataType::Date32,
    );
}

#[test]
fn date32_to_int32() {
    test_primitive_to_primitive(
        &[10000i32, 17890],
        DataType::Date32,
        &[10000i32, 17890],
        DataType::Int32,
    );
}

#[test]
fn date64_to_int32() {
    test_primitive_to_primitive(
        &[10000i64, 17890],
        DataType::Date64,
        &[10000i32, 17890],
        DataType::Int32,
    );
}

#[test]
fn date32_to_int64() {
    test_primitive_to_primitive(
        &[10000i32, 17890],
        DataType::Date32,
        &[10000i64, 17890],
        DataType::Int64,
    );
}

#[test]
fn int32_to_date32() {
    test_primitive_to_primitive(
        &[10000i32, 17890],
        DataType::Int32,
        &[10000i32, 17890],
        DataType::Date32,
    );
}

#[test]
fn timestamp_to_date32() {
    test_primitive_to_primitive(
        &[864000000005i64, 1545696000001],
        DataType::Timestamp(TimeUnit::Millisecond, Some(String::from("UTC"))),
        &[10000i32, 17890],
        DataType::Date32,
    );
}

#[test]
fn timestamp_to_date64() {
    test_primitive_to_primitive(
        &[864000000005i64, 1545696000001],
        DataType::Timestamp(TimeUnit::Millisecond, Some(String::from("UTC"))),
        &[864000000005i64, 1545696000001i64],
        DataType::Date64,
    );
}

#[test]
fn timestamp_to_i64() {
    test_primitive_to_primitive(
        &[864000000005i64, 1545696000001],
        DataType::Timestamp(TimeUnit::Millisecond, Some(String::from("UTC"))),
        &[864000000005i64, 1545696000001i64],
        DataType::Int64,
    );
}

#[test]
fn timestamp_to_timestamp() {
    test_primitive_to_primitive(
        &[864000003005i64, 1545696002001],
        DataType::Timestamp(TimeUnit::Millisecond, None),
        &[864000003i64, 1545696002],
        DataType::Timestamp(TimeUnit::Second, None),
    );
}

#[test]
fn utf8_to_dict() {
    let array = Utf8Array::<i32>::from(&[Some("one"), None, Some("three"), Some("one")]);

    // Cast to a dictionary (same value type, Utf8)
    let cast_type = DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8));
    let result = cast(&array, &cast_type, CastOptions::default()).expect("cast failed");

    let mut expected = MutableDictionaryArray::<u8, MutableUtf8Array<i32>>::new();
    expected
        .try_extend([Some("one"), None, Some("three"), Some("one")])
        .unwrap();
    let expected: DictionaryArray<u8> = expected.into();
    assert_eq!(expected, result.as_ref());
}

#[test]
fn dict_to_utf8() {
    let mut array = MutableDictionaryArray::<u8, MutableUtf8Array<i32>>::new();
    array
        .try_extend([Some("one"), None, Some("three"), Some("one")])
        .unwrap();
    let array: DictionaryArray<u8> = array.into();

    let result = cast(&array, &DataType::Utf8, CastOptions::default()).expect("cast failed");

    let expected = Utf8Array::<i32>::from(&[Some("one"), None, Some("three"), Some("one")]);

    assert_eq!(expected, result.as_ref());
}

#[test]
fn i32_to_dict() {
    let array = Int32Array::from(&[Some(1), None, Some(3), Some(1)]);

    // Cast to a dictionary (same value type, Utf8)
    let cast_type = DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Int32));
    let result = cast(&array, &cast_type, CastOptions::default()).expect("cast failed");

    let mut expected = MutableDictionaryArray::<u8, MutablePrimitiveArray<i32>>::new();
    expected
        .try_extend([Some(1), None, Some(3), Some(1)])
        .unwrap();
    let expected: DictionaryArray<u8> = expected.into();
    assert_eq!(expected, result.as_ref());
}

#[test]
fn list_to_list() {
    let data = vec![
        Some(vec![Some(1i32), Some(2), Some(3)]),
        None,
        Some(vec![Some(4), None, Some(6)]),
    ];

    let expected_data = data
        .iter()
        .map(|x| x.as_ref().map(|x| x.iter().map(|x| x.map(|x| x as u16))));

    let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
    array.try_extend(data.clone()).unwrap();
    let array: ListArray<i32> = array.into();

    let mut expected = MutableListArray::<i32, MutablePrimitiveArray<u16>>::new();
    expected.try_extend(expected_data).unwrap();
    let expected: ListArray<i32> = expected.into();

    let result = cast(&array, expected.data_type(), CastOptions::default()).unwrap();
    assert_eq!(expected, result.as_ref());
}

#[test]
fn timestamp_with_tz_to_utf8() {
    let tz = "-02:00".to_string();
    let expected =
        Utf8Array::<i32>::from_slice(&["1996-12-19T16:39:57-02:00", "1996-12-19T17:39:57-02:00"]);
    let array = Int64Array::from_slice(&[851020797000000000, 851024397000000000])
        .to(DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)));

    let result = cast(&array, expected.data_type(), CastOptions::default()).expect("cast failed");
    assert_eq!(expected, result.as_ref());
}

#[test]
fn utf8_to_timestamp_with_tz() {
    let tz = "-02:00".to_string();
    let array =
        Utf8Array::<i32>::from_slice(&["1996-12-19T16:39:57-02:00", "1996-12-19T17:39:57-02:00"]);
    // the timezone is used to map the time to UTC.
    let expected = Int64Array::from_slice(&[851020797000000000, 851024397000000000])
        .to(DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)));

    let result = cast(&array, expected.data_type(), CastOptions::default()).expect("cast failed");
    assert_eq!(expected, result.as_ref());
}

#[test]
fn utf8_to_naive_timestamp() {
    let array =
        Utf8Array::<i32>::from_slice(&["1996-12-19T16:39:57-02:00", "1996-12-19T17:39:57-02:00"]);
    // the timezone is disregarded from the string and we assume UTC
    let expected = Int64Array::from_slice(&[851013597000000000, 851017197000000000])
        .to(DataType::Timestamp(TimeUnit::Nanosecond, None));

    let result = cast(&array, expected.data_type(), CastOptions::default()).expect("cast failed");
    assert_eq!(expected, result.as_ref());
}

#[test]
fn naive_timestamp_to_utf8() {
    let array = Int64Array::from_slice(&[851013597000000000, 851017197000000000])
        .to(DataType::Timestamp(TimeUnit::Nanosecond, None));

    let expected = Utf8Array::<i32>::from_slice(&["1996-12-19 16:39:57", "1996-12-19 17:39:57"]);

    let result = cast(&array, expected.data_type(), CastOptions::default()).expect("cast failed");
    assert_eq!(expected, result.as_ref());
}

/*
#[test]
fn dict_to_dict_bad_index_value_primitive() {
    use DataType::*;
    // test converting from an array that has indexes of a type
    // that are out of bounds for a particular other kind of
    // index.

    let keys_builder = PrimitiveBuilder::<i32>::new(10);
    let values_builder = PrimitiveBuilder::<i64>::new(10);
    let mut builder = PrimitiveDictionaryBuilder::new(keys_builder, values_builder);

    // add 200 distinct values (which can be stored by a
    // dictionary indexed by int32, but not a dictionary indexed
    // with int8)
    for i in 0..200 {
        builder.append(i).unwrap();
    }
    let array: ArrayRef = Arc::new(builder.finish());

    let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let res = cast(&array, &cast_type, CastOptions::default());
    assert, CastOptions::default())!(res.is_err());
    let actual_error = format!("{:?}", res);
    let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
    assert!(
        actual_error.contains(expected_error),
        "did not find expected error '{}' in actual error '{}'",
        actual_error,
        expected_error
    );
}

#[test]
fn dict_to_dict_bad_index_value_utf8() {
    use DataType::*;
    // Same test as dict_to_dict_bad_index_value but use
    // string values (and encode the expected behavior here);

    let keys_builder = PrimitiveBuilder::<i32>::new(10);
    let values_builder = StringBuilder::new(10);
    let mut builder = StringDictionaryBuilder::new(keys_builder, values_builder);

    // add 200 distinct values (which can be stored by a
    // dictionary indexed by int32, but not a dictionary indexed
    // with int8)
    for i in 0..200 {
        let val = format!("val{}", i);
        builder.append(&val).unwrap();
    }
    let array: ArrayRef = Arc::new(builder.finish());

    let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
    let res = cast(&array, &cast_type, CastOptions::default());
    assert, CastOptions::default())!(res.is_err());
    let actual_error = format!("{:?}", res);
    let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
    assert!(
        actual_error.contains(expected_error),
        "did not find expected error '{}' in actual error '{}'",
        actual_error,
        expected_error
    );
}

#[test]
fn utf8_to_date32() {
    use chrono::NaiveDate;
    let from_ymd = chrono::NaiveDate::from_ymd;
    let since = chrono::NaiveDate::signed_duration_since;

    let a = StringArray::from(vec![
        "2000-01-01",          // valid date with leading 0s
        "2000-2-2",            // valid date without leading 0s
        "2000-00-00",          // invalid month and day
        "2000-01-01T12:00:00", // date + time is invalid
        "2000",                // just a year is invalid
    ]);
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Date32, CastOptions::default()).unwrap();
    let c = b.as_any().downcast_ref::<Date32Array>().unwrap();

    // test valid inputs
    let date_value = since(NaiveDate::from_ymd(2000, 1, 1), from_ymd(1970, 1, 1))
        .num_days() as i32;
    assert_eq!(true, c.is_valid(0)); // "2000-01-01"
    assert_eq!(date_value, c.value(0));

    let date_value = since(NaiveDate::from_ymd(2000, 2, 2), from_ymd(1970, 1, 1))
        .num_days() as i32;
    assert_eq!(true, c.is_valid(1)); // "2000-2-2"
    assert_eq!(date_value, c.value(1));

    // test invalid inputs
    assert_eq!(false, c.is_valid(2)); // "2000-00-00"
    assert_eq!(false, c.is_valid(3)); // "2000-01-01T12:00:00"
    assert_eq!(false, c.is_valid(4)); // "2000"
}

#[test]
fn utf8_to_date64() {
    let a = StringArray::from(vec![
        "2000-01-01T12:00:00", // date + time valid
        "2020-12-15T12:34:56", // date + time valid
        "2020-2-2T12:34:56",   // valid date time without leading 0s
        "2000-00-00T12:00:00", // invalid month and day
        "2000-01-01 12:00:00", // missing the 'T'
        "2000-01-01",          // just a date is invalid
    ]);
    let array = Arc::new(a) as ArrayRef;
    let b = cast(&array, &DataType::Date64, CastOptions::default()).unwrap();
    let c = b.as_any().downcast_ref::<Date64Array>().unwrap();

    // test valid inputs
    assert_eq!(true, c.is_valid(0)); // "2000-01-01T12:00:00"
    assert_eq!(946728000000, c.value(0));
    assert_eq!(true, c.is_valid(1)); // "2020-12-15T12:34:56"
    assert_eq!(1608035696000, c.value(1));
    assert_eq!(true, c.is_valid(2)); // "2020-2-2T12:34:56"
    assert_eq!(1580646896000, c.value(2));

    // test invalid inputs
    assert_eq!(false, c.is_valid(3)); // "2000-00-00T12:00:00"
    assert_eq!(false, c.is_valid(4)); // "2000-01-01 12:00:00"
    assert_eq!(false, c.is_valid(5)); // "2000-01-01"
}

fn make_union_array() -> UnionArray {
    let mut builder = UnionBuilder::new_dense(7);
    builder.append::<i32>("a", 1).unwrap();
    builder.append::<i64>("b", 2).unwrap();
    builder.build().unwrap()
}
*/
