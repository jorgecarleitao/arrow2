use super::super::api;

use std::num::NonZeroUsize;

use crate::datatypes::DataType;
use crate::error::{Error, Result};

pub fn data_type_to(data_type: &DataType) -> Result<api::DataType> {
    Ok(match data_type {
        DataType::Boolean => api::DataType::Bit,
        DataType::Int16 => api::DataType::SmallInt,
        DataType::Int32 => api::DataType::Integer,
        DataType::Float32 => api::DataType::Float { precision: 24 },
        DataType::Float64 => api::DataType::Float { precision: 53 },
        DataType::FixedSizeBinary(length) => api::DataType::Binary {
            length: NonZeroUsize::new(*length),
        },
        DataType::Binary | DataType::LargeBinary => api::DataType::Varbinary {
            length: NonZeroUsize::new(1),
        },
        DataType::Utf8 | DataType::LargeUtf8 => api::DataType::Varchar {
            length: NonZeroUsize::new(1),
        },
        other => return Err(Error::nyi(format!("{other:?} to ODBC"))),
    })
}
