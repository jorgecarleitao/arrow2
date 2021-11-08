use crate::{
    array::*,
    datatypes::{DataType, IntervalUnit, TimeUnit},
    temporal_conversions,
};

macro_rules! dyn_display {
    ($array:expr, $ty:ty, $expr:expr) => {{
        let a = $array.as_any().downcast_ref::<$ty>().unwrap();
        Box::new(move |row: usize| format!("{}", $expr(a.value(row))))
    }};
}

macro_rules! dyn_primitive {
    ($array:expr, $ty:ty, $expr:expr) => {{
        dyn_display!($array, PrimitiveArray<$ty>, $expr)
    }};
}

/// Returns a function of index returning the string representation of the _value_ of `array`.
/// This does not take nulls into account.
pub fn get_value_display<'a>(array: &'a dyn Array) -> Box<dyn Fn(usize) -> String + 'a> {
    use DataType::*;
    match array.data_type() {
        Null => Box::new(|_: usize| "".to_string()),
        Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Box::new(move |row: usize| format!("{}", a.value(row)))
        }
        Int8 => dyn_primitive!(array, i8, |x| x),
        Int16 => dyn_primitive!(array, i16, |x| x),
        Int32 => dyn_primitive!(array, i32, |x| x),
        Int64 => dyn_primitive!(array, i64, |x| x),
        UInt8 => dyn_primitive!(array, u8, |x| x),
        UInt16 => dyn_primitive!(array, u16, |x| x),
        UInt32 => dyn_primitive!(array, u32, |x| x),
        UInt64 => dyn_primitive!(array, u64, |x| x),
        Float16 => unreachable!(),
        Float32 => dyn_primitive!(array, f32, |x| x),
        Float64 => dyn_primitive!(array, f64, |x| x),
        Date32 => dyn_primitive!(array, i32, temporal_conversions::date32_to_date),
        Date64 => dyn_primitive!(array, i64, temporal_conversions::date64_to_date),
        Time32(TimeUnit::Second) => {
            dyn_primitive!(array, i32, temporal_conversions::time32s_to_time)
        }
        Time32(TimeUnit::Millisecond) => {
            dyn_primitive!(array, i32, temporal_conversions::time32ms_to_time)
        }
        Time32(_) => unreachable!(), // remaining are not valid
        Time64(TimeUnit::Microsecond) => {
            dyn_primitive!(array, i64, temporal_conversions::time64us_to_time)
        }
        Time64(TimeUnit::Nanosecond) => {
            dyn_primitive!(array, i64, temporal_conversions::time64ns_to_time)
        }
        Time64(_) => unreachable!(), // remaining are not valid
        Timestamp(time_unit, tz) => {
            if let Some(tz) = tz {
                let timezone = temporal_conversions::parse_offset(tz);
                match timezone {
                    Ok(timezone) => {
                        dyn_primitive!(array, i64, |time| {
                            temporal_conversions::timestamp_to_datetime(time, *time_unit, &timezone)
                        })
                    }
                    #[cfg(feature = "chrono-tz")]
                    Err(_) => {
                        let timezone = temporal_conversions::parse_offset_tz(tz).unwrap();
                        dyn_primitive!(array, i64, |time| {
                            temporal_conversions::timestamp_to_datetime(time, *time_unit, &timezone)
                        })
                    }
                    #[cfg(not(feature = "chrono-tz"))]
                    _ => panic!(
                        "Invalid Offset format (must be [-]00:00) or chrono-tz feature not active"
                    ),
                }
            } else {
                dyn_primitive!(array, i64, |time| {
                    temporal_conversions::timestamp_to_naive_datetime(time, *time_unit)
                })
            }
        }
        Interval(IntervalUnit::YearMonth) => {
            dyn_primitive!(array, i32, |x| format!("{}m", x))
        }
        Interval(IntervalUnit::DayTime) => {
            dyn_primitive!(array, days_ms, |x: days_ms| format!(
                "{}d{}ms",
                x.days(),
                x.milliseconds()
            ))
        }

        Interval(IntervalUnit::MonthDayNano) => {
            dyn_primitive!(array, months_days_ns, |x: months_days_ns| format!(
                "{}m{}d{}ns",
                x.months(),
                x.days(),
                x.ns()
            ))
        }
        Duration(TimeUnit::Second) => dyn_primitive!(array, i64, |x| format!("{}s", x)),
        Duration(TimeUnit::Millisecond) => dyn_primitive!(array, i64, |x| format!("{}ms", x)),
        Duration(TimeUnit::Microsecond) => dyn_primitive!(array, i64, |x| format!("{}us", x)),
        Duration(TimeUnit::Nanosecond) => dyn_primitive!(array, i64, |x| format!("{}ns", x)),
        Binary => dyn_display!(array, BinaryArray<i32>, |x: &[u8]| {
            x.iter().fold("".to_string(), |mut acc, x| {
                acc.push_str(&format!("{:#010b}", x));
                acc
            })
        }),
        LargeBinary => dyn_display!(array, BinaryArray<i64>, |x: &[u8]| {
            x.iter().fold("".to_string(), |mut acc, x| {
                acc.push_str(&format!("{:#010b}", x));
                acc
            })
        }),
        FixedSizeBinary(_) => dyn_display!(array, FixedSizeBinaryArray, |x: &[u8]| {
            x.iter().fold("".to_string(), |mut acc, x| {
                acc.push_str(&format!("{:#010b}", x));
                acc
            })
        }),
        Utf8 => dyn_display!(array, Utf8Array<i32>, |x| x),
        LargeUtf8 => dyn_display!(array, Utf8Array<i64>, |x| x),
        Decimal(_, scale) => {
            // The number 999.99 has a precision of 5 and scale of 2
            let scale = *scale as u32;
            let display = move |x| {
                let base = x / 10i128.pow(scale);
                let decimals = x - base * 10i128.pow(scale);
                format!("{}.{}", base, decimals)
            };
            dyn_primitive!(array, i128, display)
        }
        List(_) => {
            let f = |x: Box<dyn Array>| {
                let display = get_value_display(x.as_ref());
                let string_values = (0..x.len()).map(display).collect::<Vec<String>>();
                format!("[{}]", string_values.join(", "))
            };
            dyn_display!(array, ListArray<i32>, f)
        }
        FixedSizeList(_, _) => {
            let f = |x: Box<dyn Array>| {
                let display = get_value_display(x.as_ref());
                let string_values = (0..x.len()).map(display).collect::<Vec<String>>();
                format!("[{}]", string_values.join(", "))
            };
            dyn_display!(array, FixedSizeListArray, f)
        }
        LargeList(_) => {
            let f = |x: Box<dyn Array>| {
                let display = get_value_display(x.as_ref());
                let string_values = (0..x.len()).map(display).collect::<Vec<String>>();
                format!("[{}]", string_values.join(", "))
            };
            dyn_display!(array, ListArray<i64>, f)
        }
        Dictionary(key_type, _) => match_integer_type!(key_type, |$T| {
            let a = array
                .as_any()
                .downcast_ref::<DictionaryArray<$T>>()
                .unwrap();
            let keys = a.keys();
            let display = get_display(a.values().as_ref());
            Box::new(move |row: usize| {
                if keys.is_null(row) {
                    "".to_string()
                }else {
                    display(keys.value(row) as usize)
                }
            })
        }),
        Map(_, _) => todo!(),
        Struct(_) => {
            let a = array.as_any().downcast_ref::<StructArray>().unwrap();
            let displays = a
                .values()
                .iter()
                .map(|x| get_value_display(x.as_ref()))
                .collect::<Vec<_>>();
            Box::new(move |row: usize| {
                let mut string = displays
                    .iter()
                    .zip(a.fields().iter().map(|f| f.name()))
                    .map(|(f, name)| (f(row), name))
                    .fold("{".to_string(), |mut acc, (v, name)| {
                        acc.push_str(&format!("{}: {}, ", name, v));
                        acc
                    });
                if string.len() > 1 {
                    // remove last ", "
                    string.pop();
                    string.pop();
                }
                string.push('}');
                string
            })
        }
        Union(_, _, _) => {
            let array = array.as_any().downcast_ref::<UnionArray>().unwrap();
            Box::new(move |row: usize| {
                let (field, index) = array.index(row);
                get_display(array.fields()[field].as_ref())(index)
            })
        }
        Extension(_, _, _) => todo!(),
    }
}

/// Returns a function of index returning the string representation of the item of `array`.
/// This outputs an empty string on nulls.
pub fn get_display<'a>(array: &'a dyn Array) -> Box<dyn Fn(usize) -> String + 'a> {
    let value_display = get_value_display(array);
    Box::new(move |row| {
        if array.is_null(row) {
            "".to_string()
        } else {
            value_display(row)
        }
    })
}
