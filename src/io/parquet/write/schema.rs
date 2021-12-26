use parquet2::{
    metadata::KeyValue,
    schema::{
        types::{
            DecimalType, IntType, LogicalType, ParquetType, PhysicalType, PrimitiveConvertedType,
            TimeType, TimeUnit as ParquetTimeUnit, TimestampType,
        },
        Repetition,
    },
};

use crate::{
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::{ArrowError, Result},
    io::ipc::write::schema_to_bytes,
    io::parquet::write::decimal_length_from_precision,
};

use super::super::ARROW_SCHEMA_META_KEY;

pub fn schema_to_metadata_key(schema: &Schema) -> KeyValue {
    let serialized_schema = schema_to_bytes(schema);

    // manually prepending the length to the schema as arrow uses the legacy IPC format
    // TODO: change after addressing ARROW-9777
    let schema_len = serialized_schema.len();
    let mut len_prefix_schema = Vec::with_capacity(schema_len + 8);
    len_prefix_schema.extend_from_slice(&[255u8, 255, 255, 255]);
    len_prefix_schema.extend_from_slice(&(schema_len as u32).to_le_bytes());
    len_prefix_schema.extend_from_slice(&serialized_schema);

    let encoded = base64::encode(&len_prefix_schema);

    KeyValue {
        key: ARROW_SCHEMA_META_KEY.to_string(),
        value: Some(encoded),
    }
}

/// Creates a [`ParquetType`] from a [`Field`].
pub fn to_parquet_type(field: &Field) -> Result<ParquetType> {
    let name = field.name().clone();
    let repetition = if field.is_nullable() {
        Repetition::Optional
    } else {
        Repetition::Required
    };
    // create type from field
    match field.data_type().to_logical_type() {
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
        // DataType::Duration(_) has no parquet representation => do not apply any logical type
        DataType::Int64 | DataType::Duration(_) => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            repetition,
            None,
            None,
            None,
        )?),
        // no natural representation in parquet; leave it as is.
        // arrow consumers MAY use the arrow schema in the metadata to parse them.
        DataType::Date64 => Ok(ParquetType::try_from_primitive(
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
        // no natural representation in parquet; leave it as is.
        // arrow consumers MAY use the arrow schema in the metadata to parse them.
        DataType::Timestamp(TimeUnit::Second, _) => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            repetition,
            None,
            None,
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
                    TimeUnit::Second => unreachable!(),
                    TimeUnit::Millisecond => ParquetTimeUnit::MILLIS(Default::default()),
                    TimeUnit::Microsecond => ParquetTimeUnit::MICROS(Default::default()),
                    TimeUnit::Nanosecond => ParquetTimeUnit::NANOS(Default::default()),
                },
            })),
            None,
        )?),
        // no natural representation in parquet; leave it as is.
        // arrow consumers MAY use the arrow schema in the metadata to parse them.
        DataType::Time32(TimeUnit::Second) => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            None,
            None,
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
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(ArrowError::InvalidArgumentError(
                    "Parquet does not support writing empty structs".to_string(),
                ));
            }
            // recursively convert children to types/nodes
            let fields = fields
                .iter()
                .map(to_parquet_type)
                .collect::<Result<Vec<_>>>()?;
            Ok(ParquetType::try_from_group(
                name, repetition, None, None, fields, None,
            )?)
        }
        DataType::Dictionary(_, value, _) => {
            let dict_field = Field::new(name.as_str(), value.as_ref().clone(), field.is_nullable());
            to_parquet_type(&dict_field)
        }
        DataType::FixedSizeBinary(size) => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::FixedLenByteArray(*size as i32),
            repetition,
            None,
            None,
            None,
        )?),
        DataType::Decimal(precision, scale) => {
            let precision = *precision;
            let scale = *scale;
            let logical_type = Some(LogicalType::DECIMAL(DecimalType {
                scale: scale as i32,
                precision: precision as i32,
            }));

            let physical_type = if precision <= 9 {
                PhysicalType::Int32
            } else if precision <= 18 {
                PhysicalType::Int64
            } else {
                let len = decimal_length_from_precision(precision) as i32;
                PhysicalType::FixedLenByteArray(len)
            };
            Ok(ParquetType::try_from_primitive(
                name,
                physical_type,
                repetition,
                Some(PrimitiveConvertedType::Decimal(
                    precision as i32,
                    scale as i32,
                )),
                logical_type,
                None,
            )?)
        }
        DataType::Interval(_) => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::FixedLenByteArray(12),
            repetition,
            Some(PrimitiveConvertedType::Interval),
            None,
            None,
        )?),
        DataType::List(f) | DataType::FixedSizeList(f, _) | DataType::LargeList(f) => {
            Ok(ParquetType::try_from_group(
                name,
                repetition,
                None,
                Some(LogicalType::LIST(Default::default())),
                vec![ParquetType::try_from_group(
                    "list".to_string(),
                    Repetition::Repeated,
                    None,
                    None,
                    vec![to_parquet_type(f)?],
                    None,
                )?],
                None,
            )?)
        }
        other => Err(ArrowError::NotYetImplemented(format!(
            "Writing the data type {:?} is not yet implemented",
            other
        ))),
    }
}
