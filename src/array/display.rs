use crate::{
    array::*,
    datatypes::{DataType, IntervalUnit, TimeUnit},
    error::{ArrowError, Result},
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

macro_rules! dyn_dict {
    ($array:expr, $ty:ty) => {{
        let a = $array
            .as_any()
            .downcast_ref::<DictionaryArray<$ty>>()
            .unwrap();
        let keys = a.keys();
        let display = get_value_display(a.values().as_ref())?;
        Box::new(move |row: usize| display(keys.value(row) as usize))
    }};
}

/// Returns a function of index returning the string representation of the array.
/// # Errors
/// This function errors iff the datatype is not yet supported for printing.
pub fn get_value_display<'a>(array: &'a dyn Array) -> Result<Box<dyn Fn(usize) -> String + 'a>> {
    use DataType::*;
    Ok(match array.data_type() {
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
        Time64(TimeUnit::Microsecond) => {
            dyn_primitive!(array, i64, temporal_conversions::time64us_to_time)
        }
        Time64(TimeUnit::Nanosecond) => {
            dyn_primitive!(array, i64, temporal_conversions::time64ns_to_time)
        }
        Timestamp(TimeUnit::Second, None) => {
            dyn_primitive!(array, i64, temporal_conversions::timestamp_s_to_datetime)
        }
        Timestamp(TimeUnit::Millisecond, None) => {
            dyn_primitive!(array, i64, temporal_conversions::timestamp_ms_to_datetime)
        }
        Timestamp(TimeUnit::Microsecond, None) => {
            dyn_primitive!(array, i64, temporal_conversions::timestamp_us_to_datetime)
        }
        Timestamp(TimeUnit::Nanosecond, None) => {
            dyn_primitive!(array, i64, temporal_conversions::timestamp_ns_to_datetime)
        }
        Timestamp(_, _) => {
            return Err(ArrowError::NotYetImplemented(
                "Priting of timestamps with timezones is not yet implemented.".to_string(),
            ))
        }
        Interval(IntervalUnit::YearMonth) => {
            dyn_primitive!(array, i32, |x| format!("{}m", x))
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
                let display = get_value_display(x.as_ref()).unwrap();
                let string_values = (0..x.len()).map(|i| display(i)).collect::<Vec<String>>();
                format!("[{}]", string_values.join(", "))
            };
            dyn_display!(array, ListArray<i32>, f)
        }
        FixedSizeList(_, _) => {
            let f = |x: Box<dyn Array>| {
                let display = get_value_display(x.as_ref()).unwrap();
                let string_values = (0..x.len()).map(|i| display(i)).collect::<Vec<String>>();
                format!("[{}]", string_values.join(", "))
            };
            dyn_display!(array, FixedSizeListArray, f)
        }
        LargeList(_) => {
            let f = |x: Box<dyn Array>| {
                let display = get_value_display(x.as_ref()).unwrap();
                let string_values = (0..x.len()).map(|i| display(i)).collect::<Vec<String>>();
                format!("[{}]", string_values.join(", "))
            };
            dyn_display!(array, ListArray<i64>, f)
        }
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => dyn_dict!(array, i8),
            DataType::Int16 => dyn_dict!(array, i16),
            DataType::Int32 => dyn_dict!(array, i32),
            DataType::Int64 => dyn_dict!(array, i64),
            DataType::UInt8 => dyn_dict!(array, u8),
            DataType::UInt16 => dyn_dict!(array, u16),
            DataType::UInt32 => dyn_dict!(array, u32),
            DataType::UInt64 => dyn_dict!(array, u64),
            _ => unreachable!(),
        },
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Priting of datatype {:?} with timezones is not yet implemented.",
                other
            )))
        }
    })
}

pub fn get_display<'a>(array: &'a dyn Array) -> Result<Box<dyn Fn(usize) -> String + 'a>> {
    let value_display = get_value_display(array)?;
    Ok(Box::new(move |row| {
        if array.is_null(row) {
            "".to_string()
        } else {
            value_display(row)
        }
    }))
}
