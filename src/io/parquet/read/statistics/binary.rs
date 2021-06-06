use std::convert::TryFrom;

use crate::datatypes::DataType;
use parquet2::schema::types::ParquetType;
use parquet2::statistics::BinaryStatistics as ParquetByteArrayStatistics;

use super::super::schema;
use super::Statistics;
use crate::error::{ArrowError, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryStatistics {
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
}

impl Statistics for BinaryStatistics {
    fn data_type(&self) -> &DataType {
        &DataType::Binary
    }
}

impl From<&ParquetByteArrayStatistics> for BinaryStatistics {
    fn from(stats: &ParquetByteArrayStatistics) -> Self {
        Self {
            null_count: stats.null_count,
            distinct_count: stats.distinct_count,
            min_value: stats.min_value.clone(),
            max_value: stats.max_value.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Utf8Statistics {
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

impl Statistics for Utf8Statistics {
    fn data_type(&self) -> &DataType {
        &DataType::Utf8
    }
}

impl TryFrom<&ParquetByteArrayStatistics> for Utf8Statistics {
    type Error = ArrowError;

    fn try_from(stats: &ParquetByteArrayStatistics) -> Result<Self> {
        Ok(Self {
            null_count: stats.null_count,
            distinct_count: stats.distinct_count,
            min_value: stats
                .min_value
                .as_ref()
                .map(|x| std::str::from_utf8(&x).map(|x| x.to_string()))
                .transpose()?,
            max_value: stats
                .max_value
                .as_ref()
                .map(|x| std::str::from_utf8(&x).map(|x| x.to_string()))
                .transpose()?,
        })
    }
}

pub(super) fn statistics_from_byte_array(
    stats: &ParquetByteArrayStatistics,
    type_: &ParquetType,
) -> Result<Box<dyn Statistics>> {
    let data_type = schema::to_data_type(type_)?.unwrap();

    use DataType::*;
    Ok(match data_type {
        Utf8 => Box::new(Utf8Statistics::try_from(stats)?),
        Binary => Box::new(BinaryStatistics::from(stats)),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Can't read {:?} from parquet",
                other
            )))
        }
    })
}
