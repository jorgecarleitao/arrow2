//! Defines the arithmetic kernels for adding a Duration to a Timestamp,
//! Time32, Time64, Date32 and Date64.
//!
//! For the purposes of Arrow Implementations, adding this value to a Timestamp
//! ("t1") naively (i.e. simply summing the two number) is acceptable even
//! though in some cases the resulting Timestamp (t2) would not account for
//! leap-seconds during the elapsed time between "t1" and "t2".  Similarly,
//! representing the difference between two Unix timestamp is acceptable, but
//! would yield a value that is possibly a few seconds off from the true
//! elapsed time.

use std::ops::{Add, Sub};

use num::cast::AsPrimitive;

use crate::{
    array::{Array, PrimitiveArray},
    compute::arity::binary,
    datatypes::{DataType, TimeUnit},
    error::{ArrowError, Result},
    temporal_conversions::{timeunit_scale, SECONDS_IN_DAY},
    types::NativeType,
};

/// Creates the scale required to add or subtract a Duration to a time array
/// (Timestamp, Time, or Date). The resulting scale always multiplies the rhs
/// number (Duration) so it can be added to the lhs number (time array).
fn create_scale(lhs: &DataType, rhs: &DataType) -> Result<f64> {
    // Matching on both data types from both numbers to calculate the correct
    // scale for the operation. The timestamp, Time and duration have a
    // Timeunit enum in its data type. This enum is used to describe the
    // addition of the duration. The Date32 and Date64 have different rules for
    // the scaling.
    let scale = match (lhs, rhs) {
        (DataType::Timestamp(timeunit_a, _), DataType::Duration(timeunit_b))
        | (DataType::Time32(timeunit_a), DataType::Duration(timeunit_b))
        | (DataType::Time64(timeunit_a), DataType::Duration(timeunit_b)) => {
            // The scale is based on the TimeUnit that each of the numbers have.
            timeunit_scale(timeunit_a, timeunit_b)
        }
        (DataType::Date32, DataType::Duration(timeunit)) => {
            // Date32 represents the time elapsed time since UNIX epoch
            // (1970-01-01) in days (32 bits). The duration value has to be
            // scaled to days to be able to add the value to the Date.
            timeunit_scale(&TimeUnit::Second, timeunit) / SECONDS_IN_DAY as f64
        }
        (DataType::Date64, DataType::Duration(timeunit)) => {
            // Date64 represents the time elapsed time since UNIX epoch
            // (1970-01-01) in milliseconds (64 bits). The duration value has
            // to be scaled to milliseconds to be able to add the value to the
            // Date.
            timeunit_scale(&TimeUnit::Millisecond, timeunit)
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(
                "Incorrect data type for the arguments".to_string(),
            ));
        }
    };

    Ok(scale)
}

/// Adds a duration to a time array (Timestamp, Time and Date). The timeunit
/// enum is used to scale correctly both arrays; adding seconds with seconds,
/// or milliseconds with milliseconds.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::time::add_duration;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::{DataType, TimeUnit};
///
/// let timestamp = Primitive::from(&vec![
///     Some(100000i64),
///     Some(200000i64),
///     None,
///     Some(300000i64),
/// ])
/// .to(DataType::Timestamp(
///     TimeUnit::Second,
///     Some("America/New_York".to_string()),
/// ));
///
/// let duration = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)])
///     .to(DataType::Duration(TimeUnit::Second));
///
/// let result = add_duration(&timestamp, &duration).unwrap();
/// let expected = Primitive::from(&vec![
///     Some(100010i64),
///     Some(200020i64),
///     None,
///     Some(300030i64),
/// ])
/// .to(DataType::Timestamp(
///     TimeUnit::Second,
///     Some("America/New_York".to_string()),
/// ));
///
/// assert_eq!(result, expected);
/// ```
pub fn add_duration<T>(
    time: &PrimitiveArray<T>,
    duration: &PrimitiveArray<i64>,
) -> Result<PrimitiveArray<T>>
where
    f64: AsPrimitive<T>,
    T: NativeType + Add<T, Output = T>,
{
    let scale = create_scale(time.data_type(), duration.data_type())?;

    // Closure for the binary operation. The closure contains the scale
    // required to add a duration to the timestamp array.
    let op = move |a: T, b: i64| a + (b as f64 * scale).as_();

    binary(time, duration, time.data_type().clone(), op)
}

/// Subtract a duration to a time array (Timestamp, Time and Date). The timeunit
/// enum is used to scale correctly both arrays; adding seconds with seconds,
/// or milliseconds with milliseconds.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::time::subtract_duration;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::{DataType, TimeUnit};
///
/// let timestamp = Primitive::from(&vec![
///     Some(100000i64),
///     Some(200000i64),
///     None,
///     Some(300000i64),
/// ])
/// .to(DataType::Timestamp(
///     TimeUnit::Second,
///     Some("America/New_York".to_string()),
/// ));
///
/// let duration = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)])
///     .to(DataType::Duration(TimeUnit::Second));
///
/// let result = subtract_duration(&timestamp, &duration).unwrap();
/// let expected = Primitive::from(&vec![
///     Some(99990i64),
///     Some(199980i64),
///     None,
///     Some(299970i64),
/// ])
/// .to(DataType::Timestamp(
///     TimeUnit::Second,
///     Some("America/New_York".to_string()),
/// ));
///
/// assert_eq!(result, expected);
///
/// ```
pub fn subtract_duration<T>(
    time: &PrimitiveArray<T>,
    duration: &PrimitiveArray<i64>,
) -> Result<PrimitiveArray<T>>
where
    f64: AsPrimitive<T>,
    T: NativeType + Sub<T, Output = T>,
{
    let scale = create_scale(time.data_type(), duration.data_type())?;

    // Closure for the binary operation. The closure contains the scale
    // required to add a duration to the timestamp array.
    let op = move |a: T, b: i64| a - (b as f64 * scale).as_();

    binary(time, duration, time.data_type().clone(), op)
}

/// Calculates the difference between two timestamps returning an array of type
/// Duration. The timeunit enum is used to scale correctly both arrays;
/// subtracting seconds with seconds, or milliseconds with milliseconds.
///
/// # Examples
/// ```
/// use arrow2::compute::arithmetics::time::subtract_timestamps;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::{DataType, TimeUnit};
/// let timestamp_a = Primitive::from(&vec![
///     Some(100_010i64),
///     Some(200_020i64),
///     None,
///     Some(300_030i64),
/// ])
/// .to(DataType::Timestamp(TimeUnit::Second, None));
///
/// let timestamp_b = Primitive::from(&vec![
///     Some(100_000i64),
///     Some(200_000i64),
///     None,
///     Some(300_000i64),
/// ])
/// .to(DataType::Timestamp(TimeUnit::Second, None));
///
/// let expected = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)])
///     .to(DataType::Duration(TimeUnit::Second));
///
/// let result = subtract_timestamps(&timestamp_a, &&timestamp_b).unwrap();
/// assert_eq!(result, expected);
/// ```
pub fn subtract_timestamps(
    lhs: &PrimitiveArray<i64>,
    rhs: &PrimitiveArray<i64>,
) -> Result<PrimitiveArray<i64>> {
    // Matching on both data types from both arrays.
    // Both timestamps have a Timeunit enum in its data type.
    // This enum is used to adjust the scale between the timestamps.
    match (lhs.data_type(), rhs.data_type()) {
        // Naive timestamp comparison. It doesn't take into account timezones
        // from the Timestamp timeunit.
        (DataType::Timestamp(timeunit_a, None), DataType::Timestamp(timeunit_b, None)) => {
            // Closure for the binary operation. The closure contains the scale
            // required to calculate the difference between the timestamps.
            let scale = timeunit_scale(timeunit_a, timeunit_b);
            let op = move |a, b| a - (b as f64 * scale) as i64;

            binary(lhs, rhs, DataType::Duration(timeunit_a.clone()), op)
        }
        _ => Err(ArrowError::InvalidArgumentError(
            "Incorrect data type for the arguments".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Primitive;
    use crate::datatypes::{DataType, TimeUnit};

    #[test]
    fn test_adding_timestamp() {
        let timestamp = Primitive::from(&vec![
            Some(100000i64),
            Some(200000i64),
            None,
            Some(300000i64),
        ])
        .to(DataType::Timestamp(
            TimeUnit::Second,
            Some("America/New_York".to_string()),
        ));

        let duration = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)])
            .to(DataType::Duration(TimeUnit::Second));

        let result = add_duration(&timestamp, &duration).unwrap();
        let expected = Primitive::from(&vec![
            Some(100010i64),
            Some(200020i64),
            None,
            Some(300030i64),
        ])
        .to(DataType::Timestamp(
            TimeUnit::Second,
            Some("America/New_York".to_string()),
        ));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_adding_duration_different_scale() {
        let timestamp = Primitive::from(&vec![
            Some(100000i64),
            Some(200000i64),
            None,
            Some(300000i64),
        ])
        .to(DataType::Timestamp(
            TimeUnit::Second,
            Some("America/New_York".to_string()),
        ));
        let expected = Primitive::from(&vec![
            Some(100010i64),
            Some(200020i64),
            None,
            Some(300030i64),
        ])
        .to(DataType::Timestamp(
            TimeUnit::Second,
            Some("America/New_York".to_string()),
        ));

        // Testing duration in milliseconds
        let duration = Primitive::from(&vec![
            Some(10_000i64),
            Some(20_000i64),
            None,
            Some(30_000i64),
        ])
        .to(DataType::Duration(TimeUnit::Millisecond));

        let result = add_duration(&timestamp, &duration).unwrap();
        assert_eq!(result, expected);

        // Testing duration in nanoseconds.
        // The last digits in the nanosecond are not significant enough to
        // be added to the timestamp which is in seconds and doesn't have a
        // fractional value
        let duration = Primitive::from(&vec![
            Some(10_000_000_999i64),
            Some(20_000_000_000i64),
            None,
            Some(30_000_000_000i64),
        ])
        .to(DataType::Duration(TimeUnit::Nanosecond));

        let result = add_duration(&timestamp, &duration).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_adding_subtract_timestamps_scale() {
        let timestamp = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)]).to(
            DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".to_string())),
        );
        let duration = Primitive::from(&vec![Some(1i64), Some(2i64), None, Some(3i64)])
            .to(DataType::Duration(TimeUnit::Second));

        let expected =
            Primitive::from(&vec![Some(1_010i64), Some(2_020i64), None, Some(3_030i64)]).to(
                DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".to_string())),
            );

        let result = add_duration(&timestamp, &duration).unwrap();
        assert_eq!(result, expected);

        let timestamp = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)]).to(
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".to_string())),
        );
        let duration = Primitive::from(&vec![Some(1i64), Some(2i64), None, Some(3i64)])
            .to(DataType::Duration(TimeUnit::Second));

        let expected = Primitive::from(&vec![
            Some(1_000_000_010i64),
            Some(2_000_000_020i64),
            None,
            Some(3_000_000_030i64),
        ])
        .to(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("America/New_York".to_string()),
        ));

        let result = add_duration(&timestamp, &duration).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtract_timestamp() {
        let timestamp = Primitive::from(&vec![
            Some(100000i64),
            Some(200000i64),
            None,
            Some(300000i64),
        ])
        .to(DataType::Timestamp(
            TimeUnit::Second,
            Some("America/New_York".to_string()),
        ));

        let duration = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)])
            .to(DataType::Duration(TimeUnit::Second));

        let result = subtract_duration(&timestamp, &duration).unwrap();
        let expected = Primitive::from(&vec![
            Some(99990i64),
            Some(199980i64),
            None,
            Some(299970i64),
        ])
        .to(DataType::Timestamp(
            TimeUnit::Second,
            Some("America/New_York".to_string()),
        ));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtracting_duration_different_scale() {
        let timestamp = Primitive::from(&vec![
            Some(100000i64),
            Some(200000i64),
            None,
            Some(300000i64),
        ])
        .to(DataType::Timestamp(
            TimeUnit::Second,
            Some("America/New_York".to_string()),
        ));
        let expected = Primitive::from(&vec![
            Some(99990i64),
            Some(199980i64),
            None,
            Some(299970i64),
        ])
        .to(DataType::Timestamp(
            TimeUnit::Second,
            Some("America/New_York".to_string()),
        ));

        // Testing duration in milliseconds
        let duration = Primitive::from(&vec![
            Some(10_000i64),
            Some(20_000i64),
            None,
            Some(30_000i64),
        ])
        .to(DataType::Duration(TimeUnit::Millisecond));

        let result = subtract_duration(&timestamp, &duration).unwrap();
        assert_eq!(result, expected);

        // Testing duration in nanoseconds.
        // The last digits in the nanosecond are not significant enough to
        // be added to the timestamp which is in seconds and doesn't have a
        // fractional value
        let duration = Primitive::from(&vec![
            Some(10_000_000_999i64),
            Some(20_000_000_000i64),
            None,
            Some(30_000_000_000i64),
        ])
        .to(DataType::Duration(TimeUnit::Nanosecond));

        let result = subtract_duration(&timestamp, &duration).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtracting_subtract_timestamps_scale() {
        let timestamp = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)]).to(
            DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".to_string())),
        );
        let duration = Primitive::from(&vec![Some(1i64), Some(2i64), None, Some(3i64)])
            .to(DataType::Duration(TimeUnit::Second));

        let expected =
            Primitive::from(&vec![Some(-990i64), Some(-1_980i64), None, Some(-2_970i64)]).to(
                DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".to_string())),
            );

        let result = subtract_duration(&timestamp, &duration).unwrap();
        assert_eq!(result, expected);

        let timestamp = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)]).to(
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".to_string())),
        );
        let duration = Primitive::from(&vec![Some(1i64), Some(2i64), None, Some(3i64)])
            .to(DataType::Duration(TimeUnit::Second));

        let expected = Primitive::from(&vec![
            Some(-999_999_990i64),
            Some(-1_999_999_980i64),
            None,
            Some(-2_999_999_970i64),
        ])
        .to(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("America/New_York".to_string()),
        ));

        let result = subtract_duration(&timestamp, &duration).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtract_timestamps() {
        let timestamp_a = Primitive::from(&vec![
            Some(100_010i64),
            Some(200_020i64),
            None,
            Some(300_030i64),
        ])
        .to(DataType::Timestamp(TimeUnit::Second, None));

        let timestamp_b = Primitive::from(&vec![
            Some(100_000i64),
            Some(200_000i64),
            None,
            Some(300_000i64),
        ])
        .to(DataType::Timestamp(TimeUnit::Second, None));

        let expected = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)])
            .to(DataType::Duration(TimeUnit::Second));

        let result = subtract_timestamps(&timestamp_a, &&timestamp_b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtract_timestamps_scale() {
        let timestamp_a = Primitive::from(&vec![
            Some(100_000_000i64),
            Some(200_000_000i64),
            None,
            Some(300_000_000i64),
        ])
        .to(DataType::Timestamp(TimeUnit::Millisecond, None));

        let timestamp_b = Primitive::from(&vec![
            Some(100_010i64),
            Some(200_020i64),
            None,
            Some(300_030i64),
        ])
        .to(DataType::Timestamp(TimeUnit::Second, None));

        let expected = Primitive::from(&vec![
            Some(-10_000i64),
            Some(-20_000i64),
            None,
            Some(-30_000i64),
        ])
        .to(DataType::Duration(TimeUnit::Millisecond));

        let result = subtract_timestamps(&timestamp_a, &&timestamp_b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_adding_to_time() {
        let duration = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)])
            .to(DataType::Duration(TimeUnit::Second));

        // Testing Time32
        let time_32 = Primitive::from(&vec![
            Some(100000i32),
            Some(200000i32),
            None,
            Some(300000i32),
        ])
        .to(DataType::Time32(TimeUnit::Second));

        let result = add_duration(&time_32, &duration).unwrap();
        let expected = Primitive::from(&vec![
            Some(100010i32),
            Some(200020i32),
            None,
            Some(300030i32),
        ])
        .to(DataType::Time32(TimeUnit::Second));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_subtract_to_time() {
        let duration = Primitive::from(&vec![Some(10i64), Some(20i64), None, Some(30i64)])
            .to(DataType::Duration(TimeUnit::Second));

        // Testing Time32
        let time_32 = Primitive::from(&vec![
            Some(100000i32),
            Some(200000i32),
            None,
            Some(300000i32),
        ])
        .to(DataType::Time32(TimeUnit::Second));

        let result = subtract_duration(&time_32, &duration).unwrap();
        let expected = Primitive::from(&vec![
            Some(99990i32),
            Some(199980i32),
            None,
            Some(299970i32),
        ])
        .to(DataType::Time32(TimeUnit::Second));

        assert_eq!(result, expected);
    }

    #[test]
    fn test_date32() {
        let duration = Primitive::from(&vec![
            Some(86_400),     // 1 day
            Some(864_000i64), // 10 days
            None,
            Some(8_640_000i64), // 100 days
        ])
        .to(DataType::Duration(TimeUnit::Second));

        let date_32 = Primitive::from(&vec![
            Some(100_000i32),
            Some(100_000i32),
            None,
            Some(100_000i32),
        ])
        .to(DataType::Date32);

        let result = add_duration(&date_32, &duration).unwrap();
        let expected = Primitive::from(&vec![
            Some(100_001i32),
            Some(100_010i32),
            None,
            Some(100_100i32),
        ])
        .to(DataType::Date32);

        assert_eq!(result, expected);

        let result = subtract_duration(&date_32, &duration).unwrap();
        let expected = Primitive::from(&vec![
            Some(99_999i32),
            Some(99_990i32),
            None,
            Some(99_900i32),
        ])
        .to(DataType::Date32);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_date64() {
        let duration = Primitive::from(&vec![
            Some(10i64),  // 10 milliseconds
            Some(100i64), // 100 milliseconds
            None,
            Some(1_000i64), // 1000 milliseconds
        ])
        .to(DataType::Duration(TimeUnit::Millisecond));

        let date_64 = Primitive::from(&vec![
            Some(100_000i64),
            Some(100_000i64),
            None,
            Some(100_000i64),
        ])
        .to(DataType::Date64);

        let result = add_duration(&date_64, &duration).unwrap();
        let expected = Primitive::from(&vec![
            Some(100_010i64),
            Some(100_100i64),
            None,
            Some(101_000i64),
        ])
        .to(DataType::Date64);

        assert_eq!(result, expected);

        let result = subtract_duration(&date_64, &duration).unwrap();
        let expected = Primitive::from(&vec![
            Some(99_990i64),
            Some(99_900i64),
            None,
            Some(99_000i64),
        ])
        .to(DataType::Date64);

        assert_eq!(result, expected);
    }
}
