use arrow2::array::*;
use arrow2::compute::temporal::*;
use arrow2::datatypes::*;

#[test]
fn date64_hour() {
    let array =
        Int64Array::from(&[Some(1514764800000), None, Some(1550636625000)]).to(DataType::Date64);

    let result = hour(&array).unwrap();
    let expected = UInt32Array::from(&[Some(0), None, Some(4)]);
    assert_eq!(result, expected);
}

#[test]
fn date32_hour() {
    let array = Int32Array::from(&[Some(15147), None, Some(15148)]).to(DataType::Date32);

    let result = hour(&array).unwrap();
    let expected = UInt32Array::from(&[Some(0), None, Some(0)]);
    assert_eq!(result, expected);
}

#[test]
fn time32_second_hour() {
    let array = Int32Array::from(&[Some(37800), None]).to(DataType::Time32(TimeUnit::Second));

    let result = hour(&array).unwrap();
    let expected = UInt32Array::from(&[Some(10), None]);
    assert_eq!(result, expected);
}

#[test]
fn time64_micro_hour() {
    let array =
        Int64Array::from(&[Some(37800000000), None]).to(DataType::Time64(TimeUnit::Microsecond));

    let result = hour(&array).unwrap();
    let expected = UInt32Array::from(&[Some(10), None]);
    assert_eq!(result, expected);
}

#[test]
fn timestamp_micro_hour() {
    let array = Int64Array::from(&[Some(37800000000), None])
        .to(DataType::Timestamp(TimeUnit::Microsecond, None));

    let result = hour(&array).unwrap();
    let expected = UInt32Array::from(&[Some(10), None]);
    assert_eq!(result, expected);
}

#[test]
fn timestamp_date64_year() {
    let array = Int64Array::from(&[Some(1514764800000), None]).to(DataType::Date64);

    let result = year(&array).unwrap();
    let expected = Int32Array::from(&[Some(2018), None]);
    assert_eq!(result, expected);
}

#[test]
fn timestamp_date32_year() {
    let array = Int32Array::from(&[Some(15147), None]).to(DataType::Date32);

    let result = year(&array).unwrap();
    let expected = Int32Array::from(&[Some(2011), None]);
    assert_eq!(result, expected);
}

#[test]
fn timestamp_micro_year() {
    let array = Int64Array::from(&[Some(1612025847000000), None])
        .to(DataType::Timestamp(TimeUnit::Microsecond, None));

    let result = year(&array).unwrap();
    let expected = Int32Array::from(&[Some(2021), None]);
    assert_eq!(result, expected);
}

#[test]
fn consistency_hour() {
    use arrow2::array::new_null_array;
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
        if can_hour(&d1) {
            assert!(hour(array.as_ref()).is_ok());
        } else {
            assert!(hour(array.as_ref()).is_err());
        }
    });
}

#[test]
fn consistency_year() {
    use arrow2::array::new_null_array;
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
        if can_year(&d1) {
            assert!(year(array.as_ref()).is_ok());
        } else {
            assert!(year(array.as_ref()).is_err());
        }
    });
}
