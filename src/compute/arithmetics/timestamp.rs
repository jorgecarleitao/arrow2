//! Defines the arithmetic kernels for Timestamp and Duration.
//! For the purposes of Arrow Implementations, adding this value to a Timestamp
//! ("t1") naively (i.e. simply summing the two number) is acceptable even
//! though in some cases the resulting Timestamp (t2) would not account for
//! leap-seconds during the elapsed time between "t1" and "t2".  Similarly,
//! representing the difference between two Unix timestamp is acceptable, but
//! would yield a value that is possibly a few seconds off from the true elapsed
//! time.

use crate::{
    array::{Array, PrimitiveArray},
    compute::arity::binary,
    datatypes::DataType,
    error::{ArrowError, Result},
    temporal_conversions::timeunit_scale,
};

/// Adds a duration to a timestamp array. The timeunit enum is used to scale
/// correctly both arrays; adding seconds with seconds, or milliseconds with
/// milliseconds.
pub fn add_duration(
    timestamp: &PrimitiveArray<i64>,
    duration: &PrimitiveArray<i64>,
) -> Result<PrimitiveArray<i64>> {
    // Matching on both data types from both arrays.
    // The timestamp and duration have a Timeunit enum in its data type.
    // This enum is used to describe the addition of the duration.
    match (timestamp.data_type(), duration.data_type()) {
        (DataType::Timestamp(timeunit_a, _), DataType::Duration(timeunit_b)) => {
            // Closure for the binary operation. The closure contains the scale
            // required to add a duration to the timestamp array.
            let scale = timeunit_scale(timeunit_a, timeunit_b);
            let op = move |a, b| a + (b as f64 * scale) as i64;

            binary(timestamp, duration, timestamp.data_type().clone(), op)
        }
        _ => Err(ArrowError::InvalidArgumentError(
            "Incorrect data type for the arguments".to_string(),
        )),
    }
}

/// Subtract a duration to a timestamp array. The timeunit enum is used to scale
/// correctly both arrays; subtracting seconds with seconds, or milliseconds with
/// milliseconds.
pub fn subtract_duration(
    timestamp: &PrimitiveArray<i64>,
    duration: &PrimitiveArray<i64>,
) -> Result<PrimitiveArray<i64>> {
    // Matching on both data types from both arrays.
    // The timestamp and duration have a Timeunit enum in its data type.
    // This enum is used to describe the addition of the duration.
    match (timestamp.data_type(), duration.data_type()) {
        (DataType::Timestamp(timeunit_a, _), DataType::Duration(timeunit_b)) => {
            // Closure for the binary operation. The closure contains the scale
            // required to add a duration to the timestamp array.
            let scale = timeunit_scale(timeunit_a, timeunit_b);
            let op = move |a, b| a - (b as f64 * scale) as i64;

            binary(timestamp, duration, timestamp.data_type().clone(), op)
        }
        _ => Err(ArrowError::InvalidArgumentError(
            "Incorrect data type for the arguments".to_string(),
        )),
    }
}

/// Calculates the difference between two timestamps returning an array of type
/// Duration. The timeunit enum is used to scale correctly both arrays;
/// subtracting seconds with seconds, or milliseconds with milliseconds.
pub fn timestamp_diff(
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
    fn test_adding_timestamp_different_scale() {
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
    fn test_subtracting_timestamp_different_scale() {
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
    fn test_timestamp_diff() {
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

        let result = timestamp_diff(&timestamp_a, &&timestamp_b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_timestamp_diff_scale() {
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

        let result = timestamp_diff(&timestamp_a, &&timestamp_b).unwrap();
        assert_eq!(result, expected);
    }
}
