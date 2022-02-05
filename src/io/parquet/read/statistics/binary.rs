use std::any::Any;
use std::convert::TryFrom;

use crate::datatypes::DataType;
use parquet2::statistics::BinaryStatistics as ParquetByteArrayStatistics;

use super::Statistics;
use crate::error::{ArrowError, Result};

/// Represents a `Binary` or `LargeBinary`
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryStatistics {
    /// number of nulls
    pub null_count: Option<i64>,
    /// number of dictinct values
    pub distinct_count: Option<i64>,
    /// Minimum
    pub min_value: Option<Vec<u8>>,
    /// Maximum
    pub max_value: Option<Vec<u8>>,
}

impl Statistics for BinaryStatistics {
    fn data_type(&self) -> &DataType {
        &DataType::Binary
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
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

/// Statistics of a string parquet column
#[derive(Debug, Clone, PartialEq)]
pub struct Utf8Statistics {
    /// number of nulls
    pub null_count: Option<i64>,
    /// number of dictinct values
    pub distinct_count: Option<i64>,
    /// Minimum
    pub min_value: Option<String>,
    /// Maximum
    pub max_value: Option<String>,
}

impl Statistics for Utf8Statistics {
    fn data_type(&self) -> &DataType {
        &DataType::Utf8
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
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
                .map(|x| simdutf8::basic::from_utf8(x).map(|x| x.to_string()))
                .transpose()?,
            max_value: stats
                .max_value
                .as_ref()
                .map(|x| simdutf8::basic::from_utf8(x).map(|x| x.to_string()))
                .transpose()?,
        })
    }
}

pub(super) fn statistics_from_byte_array(
    stats: &ParquetByteArrayStatistics,
    data_type: DataType,
) -> Result<Box<dyn Statistics>> {
    use DataType::*;
    Ok(match data_type {
        Utf8 => Box::new(Utf8Statistics::try_from(stats)?),
        LargeUtf8 => Box::new(Utf8Statistics::try_from(stats)?),
        Binary => Box::new(BinaryStatistics::from(stats)),
        LargeBinary => Box::new(BinaryStatistics::from(stats)),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Can't read {:?} from parquet",
                other
            )))
        }
    })
}
