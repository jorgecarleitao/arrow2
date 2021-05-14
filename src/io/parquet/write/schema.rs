use parquet2::schema::{
    types::{ParquetType, PhysicalType, PrimitiveConvertedType, TimeUnit as ParquetTimeUnit},
    FieldRepetitionType, IntType, LogicalType, TimeType, TimestampType,
};

use crate::datatypes::{DataType, Field, TimeUnit};
use crate::error::{ArrowError, Result};

pub fn to_parquet_type(field: &Field) -> Result<ParquetType> {
    let name = field.name().clone();
    let repetition = if field.is_nullable() {
        FieldRepetitionType::Optional
    } else {
        FieldRepetitionType::Required
    };
    // create type from field
    match field.data_type() {
        DataType::Null => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            None,
            Some(LogicalType::UNKNOWN(Default::default())),
            None,
        )?),
        DataType::Boolean => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Boolean,
            repetition,
            None,
            None,
            None,
        )?),
        DataType::Int32 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            None,
            None,
            None,
        )?),
        DataType::Int64 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            repetition,
            None,
            None,
            None,
        )?),
        DataType::Float32 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Float,
            repetition,
            None,
            None,
            None,
        )?),
        DataType::Float64 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Double,
            repetition,
            None,
            None,
            None,
        )?),
        DataType::Binary | DataType::LargeBinary => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::ByteArray,
            repetition,
            None,
            None,
            None,
        )?),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::ByteArray,
            repetition,
            Some(PrimitiveConvertedType::Utf8),
            Some(LogicalType::STRING(Default::default())),
            None,
        )?),
        DataType::Date32 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Date),
            Some(LogicalType::DATE(Default::default())),
            None,
        )?),
        DataType::Int8 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Int8),
            Some(LogicalType::INTEGER(IntType {
                bit_width: 8,
                is_signed: true,
            })),
            None,
        )?),
        DataType::Int16 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Int16),
            Some(LogicalType::INTEGER(IntType {
                bit_width: 16,
                is_signed: true,
            })),
            None,
        )?),
        DataType::UInt8 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Uint8),
            Some(LogicalType::INTEGER(IntType {
                bit_width: 8,
                is_signed: false,
            })),
            None,
        )?),
        DataType::UInt16 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Uint16),
            Some(LogicalType::INTEGER(IntType {
                bit_width: 16,
                is_signed: false,
            })),
            None,
        )?),
        DataType::UInt32 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Uint32),
            Some(LogicalType::INTEGER(IntType {
                bit_width: 32,
                is_signed: false,
            })),
            None,
        )?),
        DataType::UInt64 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            repetition,
            Some(PrimitiveConvertedType::Uint64),
            Some(LogicalType::INTEGER(IntType {
                bit_width: 64,
                is_signed: false,
            })),
            None,
        )?),
        DataType::Timestamp(time_unit, zone) => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            repetition,
            None,
            Some(LogicalType::TIMESTAMP(TimestampType {
                is_adjusted_to_u_t_c: matches!(zone, Some(z) if !z.as_str().is_empty()),
                unit: match time_unit {
                    TimeUnit::Second => ParquetTimeUnit::MILLIS(Default::default()),
                    TimeUnit::Millisecond => ParquetTimeUnit::MILLIS(Default::default()),
                    TimeUnit::Microsecond => ParquetTimeUnit::MICROS(Default::default()),
                    TimeUnit::Nanosecond => ParquetTimeUnit::NANOS(Default::default()),
                },
            })),
            None,
        )?),
        DataType::Time32(TimeUnit::Millisecond) => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::TimeMillis),
            Some(LogicalType::TIME(TimeType {
                is_adjusted_to_u_t_c: false,
                unit: ParquetTimeUnit::MILLIS(Default::default()),
            })),
            None,
        )?),
        DataType::Time64(time_unit) => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            repetition,
            match time_unit {
                TimeUnit::Microsecond => Some(PrimitiveConvertedType::TimeMicros),
                TimeUnit::Nanosecond => None,
                _ => unreachable!(),
            },
            Some(LogicalType::TIME(TimeType {
                is_adjusted_to_u_t_c: false,
                unit: match time_unit {
                    TimeUnit::Microsecond => ParquetTimeUnit::MICROS(Default::default()),
                    TimeUnit::Nanosecond => ParquetTimeUnit::NANOS(Default::default()),
                    _ => unreachable!(),
                },
            })),
            None,
        )?),
        DataType::Duration(_) => Err(ArrowError::InvalidArgumentError(
            "Converting Duration to parquet not supported".to_string(),
        )),
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(ArrowError::InvalidArgumentError(
                    "Parquet does not support writing empty structs".to_string(),
                ));
            }
            // recursively convert children to types/nodes
            let fields = fields
                .iter()
                .map(|f| to_parquet_type(f))
                .collect::<Result<Vec<_>>>()?;
            Ok(ParquetType::try_from_group(
                name, repetition, None, None, fields, None,
            )?)
        }
        DataType::Dictionary(_, value) => {
            let dict_field = Field::new(name.as_str(), value.as_ref().clone(), field.is_nullable());
            to_parquet_type(&dict_field)
        }
        DataType::FixedSizeBinary(size) => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::FixedLenByteArray(*size),
            repetition,
            None,
            None,
            None,
        )?),
        /*
        DataType::Interval(_) => {
            Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_converted_type(ConvertedType::INTERVAL)
                .with_repetition(repetition)
                .with_length(12)
                .build()
        }
        DataType::FixedSizeBinary(length) => {
            Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_repetition(repetition)
                .with_length(*length)
                .build()
        }
        DataType::Decimal(precision, scale) => {
            // Decimal precision determines the Parquet physical type to use.
            // TODO(ARROW-12018): Enable the below after ARROW-10818 Decimal support
            //
            // let (physical_type, length) = if *precision > 1 && *precision <= 9 {
            //     (PhysicalType::INT32, -1)
            // } else if *precision <= 18 {
            //     (PhysicalType::INT64, -1)
            // } else {
            //     (
            //         PhysicalType::FIXED_LEN_BYTE_ARRAY,
            //         decimal_length_from_precision(*precision) as i32,
            //     )
            // };
            Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_repetition(repetition)
                .with_length(decimal_length_from_precision(*precision) as i32)
                .with_logical_type(Some(LogicalType::DECIMAL(DecimalType {
                    scale: *scale as i32,
                    precision: *precision as i32,
                })))
                .with_precision(*precision as i32)
                .with_scale(*scale as i32)
                .build()
        }
        DataType::List(f) | DataType::FixedSizeList(f, _) | DataType::LargeList(f) => {
            Type::group_type_builder(name)
                .with_fields(&mut vec![Arc::new(
                    Type::group_type_builder("list")
                        .with_fields(&mut vec![Arc::new(arrow_to_parquet_type(f)?)])
                        .with_repetition(Repetition::REPEATED)
                        .build()?,
                )])
                .with_logical_type(Some(LogicalType::LIST(Default::default())))
                .with_repetition(repetition)
                .build()
        }
        */
        other => Err(ArrowError::NotYetImplemented(format!(
            "Writing the data type {:?} is not yet implemented",
            other
        ))),
    }
}
