use arrow2::array::*;
use arrow2::datatypes::TimeUnit;
use arrow2::temporal_conversions;
use arrow2::types::months_days_ns;

#[test]
fn naive() {
    let expected = "Timestamp(Nanosecond, None)[1996-12-19 16:39:57, 1996-12-19 13:39:57, None]";
    let fmt = "%Y-%m-%dT%H:%M:%S:z";
    let array = Utf8Array::<i32>::from_slice(&[
        "1996-12-19T16:39:57-02:00",
        "1996-12-19T13:39:57-03:00",
        "1996-12-19 13:39:57-03:00", // missing T
    ]);
    let r = temporal_conversions::utf8_to_naive_timestamp_ns(&array, fmt);
    assert_eq!(format!("{:?}", r), expected);

    let fmt = "%Y-%m-%dT%H:%M:%S"; // no tz info
    let array = Utf8Array::<i32>::from_slice(&[
        "1996-12-19T16:39:57-02:00",
        "1996-12-19T13:39:57-03:00",
        "1996-12-19 13:39:57-03:00", // missing T
    ]);
    let r = temporal_conversions::utf8_to_naive_timestamp_ns(&array, fmt);
    assert_eq!(format!("{:?}", r), expected);
}

#[test]
fn naive_no_tz() {
    let expected = "Timestamp(Nanosecond, None)[1996-12-19 16:39:57, 1996-12-19 13:39:57, None]";
    let fmt = "%Y-%m-%dT%H:%M:%S"; // no tz info
    let array = Utf8Array::<i32>::from_slice(&[
        "1996-12-19T16:39:57",
        "1996-12-19T13:39:57",
        "1996-12-19 13:39:57", // missing T
    ]);
    let r = temporal_conversions::utf8_to_naive_timestamp_ns(&array, fmt);
    assert_eq!(format!("{:?}", r), expected);
}

#[test]
fn tz_aware() {
    let tz = "-02:00".to_string();
    let expected =
        "Timestamp(Nanosecond, Some(\"-02:00\"))[1996-12-19 16:39:57 -02:00, 1996-12-19 17:39:57 -02:00, None]";
    let fmt = "%Y-%m-%dT%H:%M:%S%.f%:z";
    let array = Utf8Array::<i32>::from_slice(&[
        "1996-12-19T16:39:57.0-02:00",
        "1996-12-19T16:39:57.0-03:00", // same time at a different TZ
        "1996-12-19 13:39:57.0-03:00",
    ]);
    let r = temporal_conversions::utf8_to_timestamp_ns(&array, fmt, tz).unwrap();
    assert_eq!(format!("{:?}", r), expected);
}

#[test]
fn tz_aware_no_timezone() {
    let tz = "-02:00".to_string();
    let expected = "Timestamp(Nanosecond, Some(\"-02:00\"))[None, None, None]";
    let fmt = "%Y-%m-%dT%H:%M:%S%.f";
    let array = Utf8Array::<i32>::from_slice(&[
        "1996-12-19T16:39:57.0",
        "1996-12-19T17:39:57.0",
        "1996-12-19 13:39:57.0",
    ]);
    let r = temporal_conversions::utf8_to_timestamp_ns(&array, fmt, tz).unwrap();
    assert_eq!(format!("{:?}", r), expected);
}

#[test]
fn add_interval_fixed_offset() {
    // 1972 has a leap year on the 29th.
    let timestamp = 68086800; // Mon Feb 28 1972 01:00:00 GMT+0000
    let timeunit = TimeUnit::Second;
    let timezone = temporal_conversions::parse_offset("+01:00").unwrap();

    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(0, 1, 60_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("1972-02-29 02:01:00 +01:00", format!("{}", r));

    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(1, 1, 60_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("1972-03-29 02:01:00 +01:00", format!("{}", r));

    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(24, 1, 60_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("1974-03-01 02:01:00 +01:00", format!("{}", r));

    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(-1, 1, 60_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("1972-01-29 02:01:00 +01:00", format!("{}", r));
}

#[cfg(feature = "chrono-tz")]
#[test]
fn add_interval_timezone() {
    // current time is Sun Mar 29 2020 00:00:00 GMT+0000 (Western European Standard Time)
    // 1 hour later is Sun Mar 29 2020 02:00:00 GMT+0100 (Western European Summer Time)
    let timestamp = 1585440000;
    let timeunit = TimeUnit::Second;
    let timezone = temporal_conversions::parse_offset_tz("Europe/Lisbon").unwrap();

    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(0, 0, 60 * 60 * 1_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("2020-03-29 02:00:00 WEST", format!("{}", r));

    // crosses two summer time changes and thus adds only 1 hour
    let r = temporal_conversions::add_interval(
        timestamp,
        timeunit,
        months_days_ns::new(7, 0, 60 * 60 * 1_000_000_000),
        &timezone,
    );
    let r = temporal_conversions::timestamp_to_datetime(r, timeunit, &timezone);
    assert_eq!("2020-10-29 01:00:00 WET", format!("{}", r));
}
