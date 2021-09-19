mod basic;
mod decimal;
mod time;

use arrow2::array::new_empty_array;
use arrow2::compute::arithmetics::{arithmetic, can_arithmetic, Operator};
use arrow2::datatypes::DataType::*;
use arrow2::datatypes::{IntervalUnit, TimeUnit};

#[test]
fn consistency() {
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
        Interval(IntervalUnit::MonthDayNano),
    ];
    let operators = vec![
        Operator::Add,
        Operator::Divide,
        Operator::Subtract,
        Operator::Multiply,
        Operator::Remainder,
    ];

    let cases = datatypes
        .clone()
        .into_iter()
        .zip(operators.into_iter())
        .zip(datatypes.into_iter());

    cases.for_each(|((lhs, op), rhs)| {
        let lhs_a = new_empty_array(lhs.clone());
        let rhs_a = new_empty_array(rhs.clone());
        if can_arithmetic(&lhs, op, &rhs) {
            assert!(arithmetic(lhs_a.as_ref(), op, rhs_a.as_ref()).is_ok());
        } else {
            assert!(arithmetic(lhs_a.as_ref(), op, rhs_a.as_ref()).is_err());
        }
    });
}
