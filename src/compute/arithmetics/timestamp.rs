// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
    compute::arity::unchecked_binary,
    datatypes::{DataType, TimeUnit},
    error::{ArrowError, Result},
};

/// Calculates the scale factor between two TimeUnits. The function returns the
/// scale that should multiply the TimeUnit "b" to have the same time scale as
/// the TimeUnit "a".
fn timeunit_scale(a: &TimeUnit, b: &TimeUnit) -> f64 {
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

            unchecked_binary(timestamp, duration, timestamp.data_type().clone(), op)
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

            unchecked_binary(timestamp, duration, timestamp.data_type().clone(), op)
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
}
