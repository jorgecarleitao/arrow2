use chrono::Datelike;

use crate::temporal_conversions::EPOCH_DAYS_FROM_CE;
use crate::{datatypes::*, error::ArrowError};

use super::{BooleanParser, PrimitiveParser, Utf8Parser};

pub trait GenericParser<E>:
    PrimitiveParser<u8, E>
    + PrimitiveParser<u16, E>
    + PrimitiveParser<u32, E>
    + PrimitiveParser<u64, E>
    + PrimitiveParser<i8, E>
    + PrimitiveParser<i16, E>
    + PrimitiveParser<i32, E>
    + PrimitiveParser<i64, E>
    + PrimitiveParser<f32, E>
    + PrimitiveParser<f64, E>
    + BooleanParser<E>
    + Utf8Parser<E>
{
}

#[derive(Debug, Clone, Copy)]
pub struct DefaultParser {}

impl Default for DefaultParser {
    fn default() -> Self {
        Self {}
    }
}

impl<E: From<ArrowError> + From<chrono::ParseError>> PrimitiveParser<u8, E> for DefaultParser {}
impl<E: From<ArrowError> + From<chrono::ParseError>> PrimitiveParser<u16, E> for DefaultParser {}
impl<E: From<ArrowError> + From<chrono::ParseError>> PrimitiveParser<u32, E> for DefaultParser {}
impl<E: From<ArrowError> + From<chrono::ParseError>> PrimitiveParser<u64, E> for DefaultParser {}

impl<E: From<ArrowError> + From<chrono::ParseError>> PrimitiveParser<i8, E> for DefaultParser {}
impl<E: From<ArrowError> + From<chrono::ParseError>> PrimitiveParser<i16, E> for DefaultParser {}

impl<E: From<ArrowError> + From<chrono::ParseError> + From<std::str::Utf8Error>>
    PrimitiveParser<i32, E> for DefaultParser
{
    #[inline]
    fn parse(&self, entry: &[u8], data_type: &DataType, _: usize) -> Result<Option<i32>, E> {
        // default behavior: error if not able to parse, else `None`
        match data_type {
            DataType::Int32 => Ok(lexical_core::parse(entry).ok()),
            DataType::Date32 => {
                let date = std::str::from_utf8(entry)?;
                let date = date.parse::<chrono::NaiveDate>()?;
                Ok(Some(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE))
            }
            DataType::Time32(_) => Err(ArrowError::NotYetImplemented(
                "Reading Time32 from CSV is not yet implemented".to_string(),
            )
            .into()),
            _ => unreachable!(),
        }
    }
}

impl<E: From<ArrowError> + From<chrono::ParseError> + From<std::str::Utf8Error>>
    PrimitiveParser<i64, E> for DefaultParser
{
    #[inline]
    fn parse(&self, entry: &[u8], data_type: &DataType, _: usize) -> Result<Option<i64>, E> {
        match data_type {
            DataType::Int64 => Ok(lexical_core::parse(entry).ok()),
            DataType::Date64 => {
                let date_time = std::str::from_utf8(entry)?;
                let date_time = date_time.parse::<chrono::NaiveDateTime>()?;
                Ok(Some(date_time.timestamp_millis()))
            }
            DataType::Time64(_) => Err(ArrowError::NotYetImplemented(
                "Reading Time32 from CSV is not yet implemented".to_string(),
            )
            .into()),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                let date_time = std::str::from_utf8(entry)?;
                let date_time = date_time.parse::<chrono::NaiveDateTime>()?;
                Ok(Some(date_time.timestamp_nanos()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let date_time = std::str::from_utf8(entry)?;
                let date_time = date_time.parse::<chrono::NaiveDateTime>()?;
                Ok(Some(date_time.timestamp_nanos() / 1000))
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                let date_time = std::str::from_utf8(entry)?;
                let date_time = date_time.parse::<chrono::NaiveDateTime>()?;
                Ok(Some(date_time.timestamp_nanos() / 1_000_000))
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                let date_time = std::str::from_utf8(entry)?;
                let date_time = date_time.parse::<chrono::NaiveDateTime>()?;
                Ok(Some(date_time.timestamp_nanos() / 1_000_000_000))
            }
            DataType::Timestamp(_, _) => Err(ArrowError::NotYetImplemented(
                "Reading time-zone aware timestamp from CSV is not yet implemented".to_string(),
            )
            .into()),
            _ => unreachable!(),
        }
    }
}

impl<E: From<ArrowError> + From<chrono::ParseError>> PrimitiveParser<f32, E> for DefaultParser {}
impl<E: From<ArrowError> + From<chrono::ParseError>> PrimitiveParser<f64, E> for DefaultParser {}

impl<E> BooleanParser<E> for DefaultParser {}

impl<E> Utf8Parser<E> for DefaultParser {}

impl<E: From<ArrowError> + From<chrono::ParseError> + From<std::str::Utf8Error>> GenericParser<E>
    for DefaultParser
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_int32() {
        let parser = DefaultParser::default();

        let cases = vec![
            (b"1", Some(1)),
        ];

        for (input, output) in cases {
            let r: Result<_, ArrowError> =
                PrimitiveParser::parse(&parser, input, &DataType::Int32, 0);
            assert_eq!(r.unwrap(), output);
        }
    }

    #[test]
    fn parse_date32() {
        let parser = DefaultParser::default();

        let cases = vec![
            (b"1970-01-01", Some(0)),
            (b"2020-03-15", Some(18336)),
            (b"1945-05-08", Some(-9004)),
        ];

        for (input, output) in cases {
            let r: Result<_, ArrowError> =
                PrimitiveParser::parse(&parser, input, &DataType::Date32, 0);
            assert_eq!(r.unwrap(), output);
        }
    }

    #[test]
    fn parse_date64() {
        let parser = DefaultParser::default();

        let cases = vec![
            (b"1970-01-01T00:00:00".as_ref(), Some(0i64)),
            (b"2018-11-13T17:11:10", Some(1542129070000)),
            (b"2018-11-13T17:11:10.011", Some(1542129070011)),
            (b"1900-02-28T12:34:56", Some(-2203932304000)),
        ];

        for (input, output) in cases {
            let r: Result<_, ArrowError> =
                PrimitiveParser::parse(&parser, input, &DataType::Date64, 0);
            assert_eq!(r.unwrap(), output);
        }
    }

    #[test]
    fn test_parsing_bool() {
        let parser = DefaultParser::default();

        let cases = vec![
            (b"true".as_ref(), Some(true)),
            (b"tRUe", Some(true)),
            (b"True", Some(true)),
            (b"TRUE", Some(true)),
            (b"t", None),
            (b"T", None),
            (b"", None),
            (b"false", Some(false)),
            (b"fALse", Some(false)),
            (b"False", Some(false)),
            (b"FALSE", Some(false)),
            (b"f", None),
            (b"F", None),
            (b"", None),
        ];

        for (input, output) in cases {
            let r: Result<_, ArrowError> = BooleanParser::parse(&parser, input, 0);
            assert_eq!(r.unwrap(), output);
        }
    }

    #[test]
    fn test_parsing_float() {
        let parser = DefaultParser::default();

        let cases = vec![
            (b"12.34".as_ref(), Some(12.34f64)),
            (b"12.0", Some(12.0)),
            (b"0.0", Some(0.0)),
            (b"inf", Some(f64::INFINITY)),
            (b"-inf", Some(f64::NEG_INFINITY)),
            (b"dd", None),
            (b"", None),
        ];

        for (input, output) in cases {
            let r: Result<_, ArrowError> =
                PrimitiveParser::parse(&parser, input, &DataType::Float64, 0);
            assert_eq!(r.unwrap(), output);
        }

        let r: Result<Option<f64>, ArrowError> =
            PrimitiveParser::parse(&parser, b"nan", &DataType::Float64, 0);
        assert!(r.unwrap().unwrap().is_nan());
        let r: Result<Option<f64>, ArrowError> =
            PrimitiveParser::parse(&parser, b"NaN", &DataType::Float64, 0);
        assert!(r.unwrap().unwrap().is_nan());
    }
}
