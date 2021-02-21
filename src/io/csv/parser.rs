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

use chrono::Datelike;

use crate::temporal_conversions::EPOCH_DAYS_FROM_CE;
use crate::{datatypes::*, error::ArrowError};

use super::{BooleanParser, PrimitiveParser};

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

impl<E: From<ArrowError> + From<chrono::ParseError>> PrimitiveParser<i32, E> for DefaultParser {
    #[inline]
    fn parse(
        &self,
        string: &str,
        data_type: &DataType,
        row_number: usize,
    ) -> Result<Option<i32>, E> {
        // default behavior: error if not able to parse, else `None`
        match data_type {
            DataType::Int32 => {
                PrimitiveParser::<i32, E>::parse(self, string, data_type, row_number)
            }
            DataType::Date32 => {
                let date = string.parse::<chrono::NaiveDate>()?;
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

impl<E: From<ArrowError> + From<chrono::ParseError>> PrimitiveParser<i64, E> for DefaultParser {
    #[inline]
    fn parse(
        &self,
        string: &str,
        data_type: &DataType,
        row_number: usize,
    ) -> Result<Option<i64>, E> {
        match data_type {
            DataType::Int64 => {
                PrimitiveParser::<i64, E>::parse(self, string, data_type, row_number)
            }
            DataType::Date64 => {
                let date_time = string.parse::<chrono::NaiveDateTime>()?;
                Ok(Some(date_time.timestamp_millis()))
            }
            DataType::Time64(_) => Err(ArrowError::NotYetImplemented(
                "Reading Time32 from CSV is not yet implemented".to_string(),
            )
            .into()),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                let date_time = string.parse::<chrono::NaiveDateTime>()?;
                Ok(Some(date_time.timestamp_nanos()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let date_time = string.parse::<chrono::NaiveDateTime>()?;
                Ok(Some(date_time.timestamp_nanos() / 1000))
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                let date_time = string.parse::<chrono::NaiveDateTime>()?;
                Ok(Some(date_time.timestamp_nanos() / 1_000_000))
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                let date_time = string.parse::<chrono::NaiveDateTime>()?;
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

impl<E: From<ArrowError> + From<chrono::ParseError>> GenericParser<E> for DefaultParser {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_date32() {
        let parser = DefaultParser::default();

        let cases = vec![
            ("1970-01-01", Some(0)),
            ("2020-03-15", Some(18336)),
            ("1945-05-08", Some(-9004)),
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
            ("1970-01-01T00:00:00", Some(0i64)),
            ("2018-11-13T17:11:10", Some(1542129070000)),
            ("2018-11-13T17:11:10.011", Some(1542129070011)),
            ("1900-02-28T12:34:56", Some(-2203932304000)),
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
            ("true", Some(true)),
            ("tRUe", Some(true)),
            ("True", Some(true)),
            ("TRUE", Some(true)),
            ("t", None),
            ("T", None),
            ("", None),
            ("false", Some(false)),
            ("fALse", Some(false)),
            ("False", Some(false)),
            ("FALSE", Some(false)),
            ("f", None),
            ("F", None),
            ("", None),
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
            ("12.34", Some(12.34f64)),
            ("12.0", Some(12.0)),
            ("0.0", Some(0.0)),
            ("inf", Some(f64::INFINITY)),
            ("-inf", Some(f64::NEG_INFINITY)),
            ("dd", None),
            ("", None),
        ];

        for (input, output) in cases {
            let r: Result<_, ArrowError> =
                PrimitiveParser::parse(&parser, input, &DataType::Float64, 0);
            assert_eq!(r.unwrap(), output);
        }

        let r: Result<Option<f64>, ArrowError> =
            PrimitiveParser::parse(&parser, "nan", &DataType::Float64, 0);
        assert!(r.unwrap().unwrap().is_nan());
        let r: Result<Option<f64>, ArrowError> =
            PrimitiveParser::parse(&parser, "NaN", &DataType::Float64, 0);
        assert!(r.unwrap().unwrap().is_nan());
    }
}
