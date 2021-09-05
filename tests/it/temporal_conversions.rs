use arrow2::array::*;
use arrow2::temporal_conversions;

#[test]
fn naive() {
    let expected = "Timestamp(Nanosecond, None)[1996-12-19 16:39:57, 1996-12-19 13:39:57, ]";
    let fmt = "%Y-%m-%dT%H:%M:%S:z";
    let array = Utf8Array::<i32>::from_slice(&[
        "1996-12-19T16:39:57-02:00",
        "1996-12-19T13:39:57-03:00",
        "1996-12-19 13:39:57-03:00", // missing T
    ]);
    let r = temporal_conversions::utf8_to_naive_timestamp_ns(&array, fmt);
    assert_eq!(format!("{}", r), expected);

    let fmt = "%Y-%m-%dT%H:%M:%S"; // no tz info
    let array = Utf8Array::<i32>::from_slice(&[
        "1996-12-19T16:39:57-02:00",
        "1996-12-19T13:39:57-03:00",
        "1996-12-19 13:39:57-03:00", // missing T
    ]);
    let r = temporal_conversions::utf8_to_naive_timestamp_ns(&array, fmt);
    assert_eq!(format!("{}", r), expected);
}

#[test]
fn naive_no_tz() {
    let expected = "Timestamp(Nanosecond, None)[1996-12-19 16:39:57, 1996-12-19 13:39:57, ]";
    let fmt = "%Y-%m-%dT%H:%M:%S"; // no tz info
    let array = Utf8Array::<i32>::from_slice(&[
        "1996-12-19T16:39:57",
        "1996-12-19T13:39:57",
        "1996-12-19 13:39:57", // missing T
    ]);
    let r = temporal_conversions::utf8_to_naive_timestamp_ns(&array, fmt);
    assert_eq!(format!("{}", r), expected);
}

#[test]
fn tz_aware() {
    let tz = "-02:00".to_string();
    let expected =
        "Timestamp(Nanosecond, Some(\"-02:00\"))[1996-12-19 16:39:57 -02:00, 1996-12-19 17:39:57 -02:00, ]";
    let fmt = "%Y-%m-%dT%H:%M:%S%.f%:z";
    let array = Utf8Array::<i32>::from_slice(&[
        "1996-12-19T16:39:57.0-02:00",
        "1996-12-19T16:39:57.0-03:00", // same time at a different TZ
        "1996-12-19 13:39:57.0-03:00",
    ]);
    let r = temporal_conversions::utf8_to_timestamp_ns(&array, fmt, tz).unwrap();
    assert_eq!(format!("{}", r), expected);
}

#[test]
fn tz_aware_no_timezone() {
    let tz = "-02:00".to_string();
    let expected = "Timestamp(Nanosecond, Some(\"-02:00\"))[, , ]";
    let fmt = "%Y-%m-%dT%H:%M:%S%.f";
    let array = Utf8Array::<i32>::from_slice(&[
        "1996-12-19T16:39:57.0",
        "1996-12-19T17:39:57.0",
        "1996-12-19 13:39:57.0",
    ]);
    let r = temporal_conversions::utf8_to_timestamp_ns(&array, fmt, tz).unwrap();
    assert_eq!(format!("{}", r), expected);
}
