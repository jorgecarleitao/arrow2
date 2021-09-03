//! Conversion methods for dates and times.

use chrono::{
    format::{parse, Parsed, StrftimeItems},
    FixedOffset, NaiveDate, NaiveDateTime, NaiveTime,
};

use crate::datatypes::{DataType, TimeUnit};
use crate::error::Result;
use crate::{
    array::{Int64Array, Offset, Utf8Array},
    error::ArrowError,
};

/// Number of seconds in a day
pub const SECONDS_IN_DAY: i64 = 86_400;
/// Number of milliseconds in a second
pub const MILLISECONDS: i64 = 1_000;
/// Number of microseconds in a second
pub const MICROSECONDS: i64 = 1_000_000;
/// Number of nanoseconds in a second
pub const NANOSECONDS: i64 = 1_000_000_000;
/// Number of milliseconds in a day
pub const MILLISECONDS_IN_DAY: i64 = SECONDS_IN_DAY * MILLISECONDS;
/// Number of days between 0001-01-01 and 1970-01-01
pub const EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// converts a `i32` representing a `date32` to [`NaiveDateTime`]
#[inline]
pub fn date32_to_datetime(v: i32) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(v as i64 * SECONDS_IN_DAY, 0)
}

/// converts a `i32` representing a `date32` to [`NaiveDate`]
#[inline]
pub fn date32_to_date(days: i32) -> NaiveDate {
    NaiveDate::from_num_days_from_ce(EPOCH_DAYS_FROM_CE + days)
}

/// converts a `i64` representing a `date64` to [`NaiveDateTime`]
#[inline]
pub fn date64_to_datetime(v: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(
        // extract seconds from milliseconds
        v / MILLISECONDS,
        // discard extracted seconds and convert milliseconds to nanoseconds
        (v % MILLISECONDS * MICROSECONDS) as u32,
    )
}

/// converts a `i64` representing a `date64` to [`NaiveDate`]
#[inline]
pub fn date64_to_date(milliseconds: i64) -> NaiveDate {
    date64_to_datetime(milliseconds).date()
}

/// converts a `i32` representing a `time32(s)` to [`NaiveDateTime`]
#[inline]
pub fn time32s_to_time(v: i32) -> NaiveTime {
    NaiveTime::from_num_seconds_from_midnight(v as u32, 0)
}

/// converts a `i32` representing a `time32(ms)` to [`NaiveDateTime`]
#[inline]
pub fn time32ms_to_time(v: i32) -> NaiveTime {
    let v = v as i64;
    NaiveTime::from_num_seconds_from_midnight(
        // extract seconds from milliseconds
        (v / MILLISECONDS) as u32,
        // discard extracted seconds and convert milliseconds to
        // nanoseconds
        (v % MILLISECONDS * MICROSECONDS) as u32,
    )
}

/// converts a `i64` representing a `time64(us)` to [`NaiveDateTime`]
#[inline]
pub fn time64us_to_time(v: i64) -> NaiveTime {
    NaiveTime::from_num_seconds_from_midnight(
        // extract seconds from microseconds
        (v / MICROSECONDS) as u32,
        // discard extracted seconds and convert microseconds to
        // nanoseconds
        (v % MICROSECONDS * MILLISECONDS) as u32,
    )
}

/// converts a `i64` representing a `time64(ns)` to [`NaiveDateTime`]
#[inline]
pub fn time64ns_to_time(v: i64) -> NaiveTime {
    NaiveTime::from_num_seconds_from_midnight(
        // extract seconds from nanoseconds
        (v / NANOSECONDS) as u32,
        // discard extracted seconds
        (v % NANOSECONDS) as u32,
    )
}

/// converts a `i64` representing a `timestamp(s)` to [`NaiveDateTime`]
#[inline]
pub fn timestamp_s_to_datetime(v: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(v, 0)
}

/// converts a `i64` representing a `timestamp(ms)` to [`NaiveDateTime`]
#[inline]
pub fn timestamp_ms_to_datetime(v: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(
        // extract seconds from milliseconds
        v / MILLISECONDS,
        // discard extracted seconds and convert milliseconds to nanoseconds
        (v % MILLISECONDS * MICROSECONDS) as u32,
    )
}

/// converts a `i64` representing a `timestamp(us)` to [`NaiveDateTime`]
#[inline]
pub fn timestamp_us_to_datetime(v: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(
        // extract seconds from microseconds
        v / MICROSECONDS,
        // discard extracted seconds and convert microseconds to nanoseconds
        (v % MICROSECONDS * MILLISECONDS) as u32,
    )
}

/// converts a `i64` representing a `timestamp(ns)` to [`NaiveDateTime`]
#[inline]
pub fn timestamp_ns_to_datetime(v: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(
        // extract seconds from nanoseconds
        v / NANOSECONDS,
        // discard extracted seconds
        (v % NANOSECONDS) as u32,
    )
}

/// Calculates the scale factor between two TimeUnits. The function returns the
/// scale that should multiply the TimeUnit "b" to have the same time scale as
/// the TimeUnit "a".
pub fn timeunit_scale(a: TimeUnit, b: TimeUnit) -> f64 {
    match (a, b) {
        (TimeUnit::Second, TimeUnit::Second) => 1.0,
        (TimeUnit::Second, TimeUnit::Millisecond) => 0.001,
        (TimeUnit::Second, TimeUnit::Microsecond) => 0.000_001,
        (TimeUnit::Second, TimeUnit::Nanosecond) => 0.000_000_001,
        (TimeUnit::Millisecond, TimeUnit::Second) => 1_000.0,
        (TimeUnit::Millisecond, TimeUnit::Millisecond) => 1.0,
        (TimeUnit::Millisecond, TimeUnit::Microsecond) => 0.001,
        (TimeUnit::Millisecond, TimeUnit::Nanosecond) => 0.000_001,
        (TimeUnit::Microsecond, TimeUnit::Second) => 1_000_000.0,
        (TimeUnit::Microsecond, TimeUnit::Millisecond) => 1_000.0,
        (TimeUnit::Microsecond, TimeUnit::Microsecond) => 1.0,
        (TimeUnit::Microsecond, TimeUnit::Nanosecond) => 0.001,
        (TimeUnit::Nanosecond, TimeUnit::Second) => 1_000_000_000.0,
        (TimeUnit::Nanosecond, TimeUnit::Millisecond) => 1_000_000.0,
        (TimeUnit::Nanosecond, TimeUnit::Microsecond) => 1_000.0,
        (TimeUnit::Nanosecond, TimeUnit::Nanosecond) => 1.0,
    }
}

pub(crate) fn parse_offset(offset: &str) -> Result<FixedOffset> {
    if offset == "UTC" {
        return Ok(FixedOffset::east(0));
    }
    let mut a = offset.split(':');
    let first = a.next().map(Ok).unwrap_or_else(|| {
        Err(ArrowError::InvalidArgumentError(
            "timezone offset must be of the form [-]00:00".to_string(),
        ))
    })?;
    let last = a.next().map(Ok).unwrap_or_else(|| {
        Err(ArrowError::InvalidArgumentError(
            "timezone offset must be of the form [-]00:00".to_string(),
        ))
    })?;
    let hours: i32 = first.parse().map_err(|_| {
        ArrowError::InvalidArgumentError("timezone offset must be of the form [-]00:00".to_string())
    })?;
    let minutes: i32 = last.parse().map_err(|_| {
        ArrowError::InvalidArgumentError("timezone offset must be of the form [-]00:00".to_string())
    })?;

    Ok(FixedOffset::east(hours * 60 * 60 + minutes * 60))
}

// not public to not expose TimeZone
#[inline]
pub(crate) fn utf8_to_timestamp_ns_scalar<T: chrono::TimeZone>(
    value: &str,
    fmt: &str,
    tz: &T,
) -> Option<i64> {
    let mut parsed = Parsed::new();
    let fmt = StrftimeItems::new(fmt);
    let r = parse(&mut parsed, value, fmt).ok();
    if r.is_some() {
        parsed
            .to_datetime_with_timezone(tz)
            .map(|x| x.timestamp_nanos())
            .ok()
    } else {
        None
    }
}

#[inline]
pub fn utf8_to_naive_timestamp_ns_scalar(value: &str, fmt: &str) -> Option<i64> {
    let fmt = StrftimeItems::new(fmt);
    let mut parsed = Parsed::new();
    parse(&mut parsed, value, fmt.clone()).ok();
    parsed
        .to_naive_datetime_with_offset(0)
        .map(|x| x.timestamp_nanos())
        .ok()
}

fn utf8_to_timestamp_ns_impl<O: Offset, T: chrono::TimeZone>(
    array: &Utf8Array<O>,
    fmt: &str,
    timezone: String,
    tz: T,
) -> Int64Array {
    let iter = array
        .iter()
        .map(|x| x.and_then(|x| utf8_to_timestamp_ns_scalar(x, fmt, &tz)));

    Int64Array::from_trusted_len_iter(iter)
        .to(DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)))
}

#[cfg(feature = "chrono-tz")]
fn chrono_tz_utf_to_timestamp_ns<O: Offset>(
    array: &Utf8Array<O>,
    fmt: &str,
    timezone: String,
) -> Result<Int64Array> {
    let tz = timezone.as_str().parse::<chrono_tz::Tz>();
    if let Ok(tz) = tz {
        Ok(utf8_to_timestamp_ns_impl(array, fmt, timezone, tz))
    } else {
        Err(ArrowError::InvalidArgumentError(format!(
            "timezone \"{}\" cannot be parsed",
            timezone
        )))
    }
}

#[cfg(not(feature = "chrono-tz"))]
fn chrono_tz_utf_to_timestamp_ns<O: Offset>(
    _: &Utf8Array<O>,
    _: &str,
    timezone: String,
) -> Result<Int64Array> {
    Err(ArrowError::InvalidArgumentError(format!(
        "timezone \"{}\" cannot be parsed (feature chrono-tz is not active)",
        timezone
    )))
}

/// Parses a [`Utf8Array`] to a time-aware timestamp, i.e. [`Int64Array`] with type `Timestamp(Nanosecond, Some(timezone))`.
/// When the value represents a string with another timezone, a conversion is applied.
/// Null elements remain null; non-parsable elements are set to null.
/// # Error
/// This function errors iff `timezone` is not parsiable to an offset.
pub fn utf8_to_timestamp_ns<O: Offset>(
    array: &Utf8Array<O>,
    fmt: &str,
    timezone: String,
) -> Result<Int64Array> {
    let tz = parse_offset(timezone.as_str());

    if let Ok(tz) = tz {
        Ok(utf8_to_timestamp_ns_impl(array, fmt, timezone, tz))
    } else {
        chrono_tz_utf_to_timestamp_ns(array, fmt, timezone)
    }
}

/// Parses a [`Utf8Array`] to naive timestamp, i.e. [`Int64Array`] with type `Timestamp(Nanosecond, None)`.
/// Timezones are ignored.
/// Null elements remain null; non-parsable elements are set to null.
pub fn utf8_to_naive_timestamp_ns<O: Offset>(array: &Utf8Array<O>, fmt: &str) -> Int64Array {
    let iter = array
        .iter()
        .map(|x| x.and_then(|x| utf8_to_naive_timestamp_ns_scalar(x, fmt)));

    Int64Array::from_trusted_len_iter(iter).to(DataType::Timestamp(TimeUnit::Nanosecond, None))
}
