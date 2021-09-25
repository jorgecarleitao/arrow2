use std::iter::FromIterator;

use arrow2::{
    array::*,
    bitmap::Bitmap,
    buffer::Buffer,
    datatypes::*,
    types::{days_ms, months_days_ns},
};

mod mutable;

#[test]
fn basics() {
    let data = vec![Some(1), None, Some(10)];

    let array = Int32Array::from_iter(data);

    assert_eq!(array.value(0), 1);
    assert_eq!(array.value(1), 0);
    assert_eq!(array.value(2), 10);
    assert_eq!(array.values().as_slice(), &[1, 0, 10]);
    assert_eq!(
        array.validity(),
        Some(&Bitmap::from_u8_slice(&[0b00000101], 3))
    );
    assert!(array.is_valid(0));
    assert!(!array.is_valid(1));
    assert!(array.is_valid(2));

    let array2 = Int32Array::from_data(
        DataType::Int32,
        array.values().clone(),
        array.validity().cloned(),
    );
    assert_eq!(array, array2);

    let array = array.slice(1, 2);
    assert_eq!(array.value(0), 0);
    assert_eq!(array.value(1), 10);
    assert_eq!(array.values().as_slice(), &[0, 10]);

    unsafe {
        assert_eq!(array.value_unchecked(0), 0);
        assert_eq!(array.value_unchecked(1), 10);
    }
}

#[test]
fn empty() {
    let array = Int32Array::new_empty(DataType::Int32);
    assert_eq!(array.values().len(), 0);
    assert_eq!(array.validity(), None);
}

#[test]
fn from() {
    let data = vec![Some(1), None, Some(10)];

    let array = PrimitiveArray::from(data.clone());
    assert_eq!(array.len(), 3);

    let array = PrimitiveArray::from_iter(data.clone());
    assert_eq!(array.len(), 3);

    let array = PrimitiveArray::from_trusted_len_iter(data.into_iter());
    assert_eq!(array.len(), 3);

    let data = vec![1i32, 2, 3];

    let array = PrimitiveArray::from_values(data.clone());
    assert_eq!(array.len(), 3);

    let array = PrimitiveArray::from_trusted_len_values_iter(data.into_iter());
    assert_eq!(array.len(), 3);
}

#[test]
fn display_int32() {
    let array = Int32Array::from(&[Some(1), None, Some(2)]);
    assert_eq!(format!("{}", array), "Int32[1, , 2]");
}

#[test]
fn display_date32() {
    let array = Int32Array::from(&[Some(1), None, Some(2)]).to(DataType::Date32);
    assert_eq!(format!("{}", array), "Date32[1970-01-02, , 1970-01-03]");
}

#[test]
fn display_time32s() {
    let array = Int32Array::from(&[Some(1), None, Some(2)]).to(DataType::Time32(TimeUnit::Second));
    assert_eq!(format!("{}", array), "Time32(Second)[00:00:01, , 00:00:02]");
}

#[test]
fn display_time32ms() {
    let array =
        Int32Array::from(&[Some(1), None, Some(2)]).to(DataType::Time32(TimeUnit::Millisecond));
    assert_eq!(
        format!("{}", array),
        "Time32(Millisecond)[00:00:00.001, , 00:00:00.002]"
    );
}

#[test]
fn display_interval_d() {
    let array =
        Int32Array::from(&[Some(1), None, Some(2)]).to(DataType::Interval(IntervalUnit::YearMonth));
    assert_eq!(format!("{}", array), "Interval(YearMonth)[1m, , 2m]");
}

#[test]
fn display_int64() {
    let array = Int64Array::from(&[Some(1), None, Some(2)]).to(DataType::Int64);
    assert_eq!(format!("{}", array), "Int64[1, , 2]");
}

#[test]
fn display_date64() {
    let array = Int64Array::from(&[Some(1), None, Some(86400000)]).to(DataType::Date64);
    assert_eq!(format!("{}", array), "Date64[1970-01-01, , 1970-01-02]");
}

#[test]
fn display_time64us() {
    let array =
        Int64Array::from(&[Some(1), None, Some(2)]).to(DataType::Time64(TimeUnit::Microsecond));
    assert_eq!(
        format!("{}", array),
        "Time64(Microsecond)[00:00:00.000001, , 00:00:00.000002]"
    );
}

#[test]
fn display_time64ns() {
    let array =
        Int64Array::from(&[Some(1), None, Some(2)]).to(DataType::Time64(TimeUnit::Nanosecond));
    assert_eq!(
        format!("{}", array),
        "Time64(Nanosecond)[00:00:00.000000001, , 00:00:00.000000002]"
    );
}

#[test]
fn display_timestamp_s() {
    let array =
        Int64Array::from(&[Some(1), None, Some(2)]).to(DataType::Timestamp(TimeUnit::Second, None));
    assert_eq!(
        format!("{}", array),
        "Timestamp(Second, None)[1970-01-01 00:00:01, , 1970-01-01 00:00:02]"
    );
}

#[test]
fn display_timestamp_ms() {
    let array = Int64Array::from(&[Some(1), None, Some(2)])
        .to(DataType::Timestamp(TimeUnit::Millisecond, None));
    assert_eq!(
        format!("{}", array),
        "Timestamp(Millisecond, None)[1970-01-01 00:00:00.001, , 1970-01-01 00:00:00.002]"
    );
}

#[test]
fn display_timestamp_us() {
    let array = Int64Array::from(&[Some(1), None, Some(2)])
        .to(DataType::Timestamp(TimeUnit::Microsecond, None));
    assert_eq!(
        format!("{}", array),
        "Timestamp(Microsecond, None)[1970-01-01 00:00:00.000001, , 1970-01-01 00:00:00.000002]"
    );
}

#[test]
fn display_timestamp_ns() {
    let array = Int64Array::from(&[Some(1), None, Some(2)])
        .to(DataType::Timestamp(TimeUnit::Nanosecond, None));
    assert_eq!(
        format!("{}", array),
        "Timestamp(Nanosecond, None)[1970-01-01 00:00:00.000000001, , 1970-01-01 00:00:00.000000002]"
    );
}

#[test]
fn display_timestamp_tz_ns() {
    let array = Int64Array::from(&[Some(1), None, Some(2)]).to(DataType::Timestamp(
        TimeUnit::Nanosecond,
        Some("+02:00".to_string()),
    ));
    assert_eq!(
        format!("{}", array),
        "Timestamp(Nanosecond, Some(\"+02:00\"))[1970-01-01 02:00:00.000000001 +02:00, , 1970-01-01 02:00:00.000000002 +02:00]"
    );
}

#[test]
fn display_duration_ms() {
    let array =
        Int64Array::from(&[Some(1), None, Some(2)]).to(DataType::Duration(TimeUnit::Millisecond));
    assert_eq!(format!("{}", array), "Duration(Millisecond)[1ms, , 2ms]");
}

#[test]
fn display_duration_s() {
    let array =
        Int64Array::from(&[Some(1), None, Some(2)]).to(DataType::Duration(TimeUnit::Second));
    assert_eq!(format!("{}", array), "Duration(Second)[1s, , 2s]");
}

#[test]
fn display_duration_us() {
    let array =
        Int64Array::from(&[Some(1), None, Some(2)]).to(DataType::Duration(TimeUnit::Microsecond));
    assert_eq!(format!("{}", array), "Duration(Microsecond)[1us, , 2us]");
}

#[test]
fn display_duration_ns() {
    let array =
        Int64Array::from(&[Some(1), None, Some(2)]).to(DataType::Duration(TimeUnit::Nanosecond));
    assert_eq!(format!("{}", array), "Duration(Nanosecond)[1ns, , 2ns]");
}

#[test]
fn display_decimal() {
    let array = Int128Array::from(&[Some(12345), None, Some(23456)]).to(DataType::Decimal(5, 2));
    assert_eq!(format!("{}", array), "Decimal(5, 2)[123.45, , 234.56]");
}

#[test]
fn display_decimal1() {
    let array = Int128Array::from(&[Some(12345), None, Some(23456)]).to(DataType::Decimal(5, 1));
    assert_eq!(format!("{}", array), "Decimal(5, 1)[1234.5, , 2345.6]");
}

#[test]
fn display_interval_days_ms() {
    let array = DaysMsArray::from(&[Some(days_ms::new(1, 1)), None, Some(days_ms::new(2, 2))]);
    assert_eq!(format!("{}", array), "Interval(DayTime)[1d1ms, , 2d2ms]");
}

#[test]
fn display_months_days_ns() {
    let data = &[
        Some(months_days_ns::new(1, 1, 2)),
        None,
        Some(months_days_ns::new(2, 3, 3)),
    ];

    let array = MonthsDaysNsArray::from(&data);

    assert_eq!(
        format!("{}", array),
        "Interval(MonthDayNano)[1m1d2ns, , 2m3d3ns]"
    );
}

#[test]
fn months_days_ns() {
    let data = &[
        months_days_ns::new(1, 1, 2),
        months_days_ns::new(1, 1, 3),
        months_days_ns::new(2, 3, 3),
    ];

    let array = MonthsDaysNsArray::from_slice(&data);

    let a = array.values().as_slice();
    assert_eq!(a, data.as_ref());
}

#[test]
#[should_panic]
fn wrong_data_type() {
    let values = Buffer::from(b"abbb");
    PrimitiveArray::from_data(DataType::Utf8, values, None);
}
