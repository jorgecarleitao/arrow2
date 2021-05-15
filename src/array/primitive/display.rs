use crate::{datatypes::*, temporal_conversions, types::days_ms};

use super::super::{display_fmt, Array};
use super::PrimitiveArray;

impl std::fmt::Display for PrimitiveArray<i32> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let new_lines = false;
        let head = &format!("{}", self.data_type());
        match self.data_type() {
            DataType::Int32 => display_fmt(self.iter(), head, f, new_lines),
            DataType::Date32 => display_fmt(
                self.iter()
                    .map(|x| x.copied().map(temporal_conversions::date32_to_date)),
                head,
                f,
                new_lines,
            ),
            DataType::Time32(TimeUnit::Second) => display_fmt(
                self.iter()
                    .map(|x| x.copied().map(temporal_conversions::time32s_to_time)),
                head,
                f,
                new_lines,
            ),
            DataType::Time32(TimeUnit::Millisecond) => display_fmt(
                self.iter()
                    .map(|x| x.copied().map(temporal_conversions::time32ms_to_time)),
                head,
                f,
                new_lines,
            ),
            DataType::Interval(IntervalUnit::YearMonth) => display_fmt(
                self.iter().map(|x| x.map(|x| format!("{}d", x))),
                head,
                f,
                new_lines,
            ),
            _ => unreachable!(),
        }
    }
}

impl std::fmt::Display for PrimitiveArray<i64> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let new_lines = false;
        let head = &format!("{}", self.data_type());
        match self.data_type() {
            DataType::Int64 => display_fmt(self.iter(), head, f, new_lines),
            DataType::Date64 => display_fmt(
                self.iter()
                    .map(|x| x.copied().map(temporal_conversions::date64_to_date)),
                head,
                f,
                new_lines,
            ),
            DataType::Time64(TimeUnit::Microsecond) => display_fmt(
                self.iter()
                    .map(|x| x.copied().map(temporal_conversions::time64us_to_time)),
                head,
                f,
                new_lines,
            ),
            DataType::Time64(TimeUnit::Nanosecond) => display_fmt(
                self.iter()
                    .map(|x| x.copied().map(temporal_conversions::time64ns_to_time)),
                head,
                f,
                new_lines,
            ),
            DataType::Timestamp(TimeUnit::Second, None) => display_fmt(
                self.iter().map(|x| {
                    x.copied()
                        .map(temporal_conversions::timestamp_s_to_datetime)
                }),
                head,
                f,
                new_lines,
            ),
            DataType::Timestamp(TimeUnit::Millisecond, None) => display_fmt(
                self.iter().map(|x| {
                    x.copied()
                        .map(temporal_conversions::timestamp_ms_to_datetime)
                }),
                head,
                f,
                new_lines,
            ),
            DataType::Timestamp(TimeUnit::Microsecond, None) => display_fmt(
                self.iter().map(|x| {
                    x.copied()
                        .map(temporal_conversions::timestamp_us_to_datetime)
                }),
                head,
                f,
                new_lines,
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => display_fmt(
                self.iter().map(|x| {
                    x.copied()
                        .map(temporal_conversions::timestamp_ns_to_datetime)
                }),
                head,
                f,
                new_lines,
            ),
            DataType::Duration(unit) => {
                let unit = match unit {
                    TimeUnit::Second => "s",
                    TimeUnit::Millisecond => "ms",
                    TimeUnit::Microsecond => "us",
                    TimeUnit::Nanosecond => "ns",
                };
                display_fmt(
                    self.iter()
                        .map(|x| x.copied().map(|x| format!("{}{}", x, unit))),
                    head,
                    f,
                    new_lines,
                )
            }
            // todo
            DataType::Timestamp(_, Some(_)) => display_fmt(self.iter(), head, f, new_lines),
            _ => unreachable!(),
        }
    }
}

impl std::fmt::Display for PrimitiveArray<i128> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.data_type() {
            DataType::Decimal(_, scale) => {
                let new_lines = false;
                let head = &format!("{}", self.data_type());
                // The number 999.99 has a precision of 5 and scale of 2
                let iter = self.iter().map(|x| {
                    x.copied().map(|x| {
                        let base = x / 10i128.pow(*scale as u32);
                        let decimals = x - base * 10i128.pow(*scale as u32);
                        format!("{}.{}", base, decimals)
                    })
                });
                display_fmt(iter, head, f, new_lines)
            }
            _ => unreachable!(),
        }
    }
}

impl std::fmt::Display for PrimitiveArray<days_ms> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let new_lines = false;
        let head = &format!("{}", self.data_type());
        let iter = self.iter().map(|x| {
            x.copied()
                .map(|x| format!("{}d{}ms", x.days(), x.milliseconds()))
        });
        display_fmt(iter, head, f, new_lines)
    }
}

macro_rules! display {
    ($ty:ty) => {
        impl std::fmt::Display for PrimitiveArray<$ty> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let head = &format!("{}", self.data_type());
                display_fmt(self.iter(), head, f, false)
            }
        }
    };
}

display!(i8);
display!(i16);
display!(u8);
display!(u16);
display!(u32);
display!(u64);
display!(f32);
display!(f64);

#[cfg(test)]
mod tests {
    use super::super::Primitive;
    use super::*;

    #[test]
    fn display_int32() {
        let array = Primitive::<i32>::from(&[Some(1), None, Some(2)]).to(DataType::Int32);
        assert_eq!(format!("{}", array), "Int32[1, , 2]");
    }

    #[test]
    fn display_date32() {
        let array = Primitive::<i32>::from(&[Some(1), None, Some(2)]).to(DataType::Date32);
        assert_eq!(format!("{}", array), "Date32[0001-01-01, , 0001-01-02]");
    }

    #[test]
    fn display_time32s() {
        let array = Primitive::<i32>::from(&[Some(1), None, Some(2)])
            .to(DataType::Time32(TimeUnit::Second));
        assert_eq!(format!("{}", array), "Time32(Second)[00:00:01, , 00:00:02]");
    }

    #[test]
    fn display_time32ms() {
        let array = Primitive::<i32>::from(&[Some(1), None, Some(2)])
            .to(DataType::Time32(TimeUnit::Millisecond));
        assert_eq!(
            format!("{}", array),
            "Time32(Millisecond)[00:00:00.001, , 00:00:00.002]"
        );
    }

    #[test]
    fn display_interval_d() {
        let array = Primitive::<i32>::from(&[Some(1), None, Some(2)])
            .to(DataType::Interval(IntervalUnit::YearMonth));
        assert_eq!(format!("{}", array), "Interval(YearMonth)[1d, , 2d]");
    }

    #[test]
    fn display_int64() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)]).to(DataType::Int64);
        assert_eq!(format!("{}", array), "Int64[1, , 2]");
    }

    #[test]
    fn display_date64() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(86400000)]).to(DataType::Date64);
        assert_eq!(format!("{}", array), "Date64[1970-01-01, , 1970-01-02]");
    }

    #[test]
    fn display_time64us() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)])
            .to(DataType::Time64(TimeUnit::Microsecond));
        assert_eq!(
            format!("{}", array),
            "Time64(Microsecond)[00:00:00.000001, , 00:00:00.000002]"
        );
    }

    #[test]
    fn display_time64ns() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)])
            .to(DataType::Time64(TimeUnit::Nanosecond));
        assert_eq!(
            format!("{}", array),
            "Time64(Nanosecond)[00:00:00.000000001, , 00:00:00.000000002]"
        );
    }

    #[test]
    fn display_timestamp_s() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)])
            .to(DataType::Timestamp(TimeUnit::Second, None));
        assert_eq!(
            format!("{}", array),
            "Timestamp(Second, None)[1970-01-01 00:00:01, , 1970-01-01 00:00:02]"
        );
    }

    #[test]
    fn display_timestamp_ms() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)])
            .to(DataType::Timestamp(TimeUnit::Millisecond, None));
        assert_eq!(
            format!("{}", array),
            "Timestamp(Millisecond, None)[1970-01-01 00:00:00.001, , 1970-01-01 00:00:00.002]"
        );
    }

    #[test]
    fn display_timestamp_us() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)])
            .to(DataType::Timestamp(TimeUnit::Microsecond, None));
        assert_eq!(
            format!("{}", array),
            "Timestamp(Microsecond, None)[1970-01-01 00:00:00.000001, , 1970-01-01 00:00:00.000002]"
        );
    }

    #[test]
    fn display_timestamp_ns() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)])
            .to(DataType::Timestamp(TimeUnit::Nanosecond, None));
        assert_eq!(
            format!("{}", array),
            "Timestamp(Nanosecond, None)[1970-01-01 00:00:00.000000001, , 1970-01-01 00:00:00.000000002]"
        );
    }

    #[test]
    fn display_duration_ms() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)])
            .to(DataType::Duration(TimeUnit::Millisecond));
        assert_eq!(format!("{}", array), "Duration(Millisecond)[1ms, , 2ms]");
    }

    #[test]
    fn display_duration_s() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)])
            .to(DataType::Duration(TimeUnit::Second));
        assert_eq!(format!("{}", array), "Duration(Second)[1s, , 2s]");
    }

    #[test]
    fn display_duration_us() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)])
            .to(DataType::Duration(TimeUnit::Microsecond));
        assert_eq!(format!("{}", array), "Duration(Microsecond)[1us, , 2us]");
    }

    #[test]
    fn display_duration_ns() {
        let array = Primitive::<i64>::from(&[Some(1), None, Some(2)])
            .to(DataType::Duration(TimeUnit::Nanosecond));
        assert_eq!(format!("{}", array), "Duration(Nanosecond)[1ns, , 2ns]");
    }

    #[test]
    fn display_decimal() {
        let array =
            Primitive::<i128>::from(&[Some(12345), None, Some(23456)]).to(DataType::Decimal(5, 2));
        assert_eq!(format!("{}", array), "Decimal(5, 2)[123.45, , 234.56]");
    }

    #[test]
    fn display_decimal1() {
        let array =
            Primitive::<i128>::from(&[Some(12345), None, Some(23456)]).to(DataType::Decimal(5, 1));
        assert_eq!(format!("{}", array), "Decimal(5, 1)[1234.5, , 2345.6]");
    }

    #[test]
    fn display_interval_days_ms() {
        let array =
            Primitive::<days_ms>::from(&[Some(days_ms::new(1, 1)), None, Some(days_ms::new(2, 2))])
                .to(DataType::Interval(IntervalUnit::DayTime));
        assert_eq!(format!("{}", array), "Interval(DayTime)[1d1ms, , 2d2ms]");
    }
}
