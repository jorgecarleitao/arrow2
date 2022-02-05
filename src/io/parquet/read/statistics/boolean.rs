use crate::datatypes::DataType;
use parquet2::statistics::BooleanStatistics as ParquetBooleanStatistics;
use std::any::Any;

use super::Statistics;

/// Statistics of a boolean parquet column
#[derive(Debug, Clone, PartialEq)]
pub struct BooleanStatistics {
    /// number of nulls
    pub null_count: Option<i64>,
    /// number of dictinct values
    pub distinct_count: Option<i64>,
    /// Minimum
    pub min_value: Option<bool>,
    /// Maximum
    pub max_value: Option<bool>,
}

impl Statistics for BooleanStatistics {
    fn data_type(&self) -> &DataType {
        &DataType::Boolean
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

impl From<&ParquetBooleanStatistics> for BooleanStatistics {
    fn from(stats: &ParquetBooleanStatistics) -> Self {
        Self {
            null_count: stats.null_count,
            distinct_count: stats.distinct_count,
            min_value: stats.min_value,
            max_value: stats.max_value,
        }
    }
}
