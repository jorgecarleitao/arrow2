use crate::datatypes::DataType;
use parquet2::statistics::BooleanStatistics as ParquetBooleanStatistics;

use super::Statistics;

#[derive(Debug, Clone, PartialEq)]
pub struct BooleanStatistics {
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min_value: Option<bool>,
    pub max_value: Option<bool>,
}

impl Statistics for BooleanStatistics {
    fn data_type(&self) -> &DataType {
        &DataType::Boolean
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
