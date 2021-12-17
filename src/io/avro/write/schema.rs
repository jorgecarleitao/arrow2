use avro_schema::{
    Field as AvroField, Fixed, FixedLogical, IntLogical, LongLogical, Record, Schema as AvroSchema,
};

use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// Converts a [`Schema`] to an avro [`AvroSchema::Record`] with it.
pub fn to_avro_schema(schema: &Schema) -> Result<AvroSchema> {
    let fields = schema
        .fields
        .iter()
        .map(|field| field_to_field(field))
        .collect::<Result<Vec<_>>>()?;
    Ok(avro_schema::Schema::Record(Record::new("", fields)))
}

fn field_to_field(field: &Field) -> Result<AvroField> {
    let schema = type_to_schema(field.data_type())?;
    Ok(AvroField::new(field.name(), schema))
}

fn type_to_schema(data_type: &DataType) -> Result<AvroSchema> {
    Ok(match data_type.to_logical_type() {
        DataType::Null => AvroSchema::Null,
        DataType::Boolean => AvroSchema::Int(None),
        DataType::Int64 => AvroSchema::Long(None),
        DataType::Float32 => AvroSchema::Float,
        DataType::Float64 => AvroSchema::Double,
        DataType::Binary => AvroSchema::Bytes(None),
        DataType::Utf8 => AvroSchema::String(None),
        DataType::List(inner) => AvroSchema::Array(Box::new(type_to_schema(inner.data_type())?)),
        DataType::Date32 => AvroSchema::Int(Some(IntLogical::Date)),
        DataType::Time32(TimeUnit::Millisecond) => AvroSchema::Int(Some(IntLogical::Time)),
        DataType::Time64(TimeUnit::Microsecond) => AvroSchema::Long(Some(LongLogical::Time)),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            AvroSchema::Long(Some(LongLogical::LocalTimestampMillis))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            AvroSchema::Long(Some(LongLogical::LocalTimestampMicros))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let mut fixed = Fixed::new("", 12);
            fixed.logical = Some(FixedLogical::Duration);
            AvroSchema::Fixed(fixed)
        }
        DataType::FixedSizeBinary(size) => AvroSchema::Fixed(Fixed::new("", *size)),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "write {:?} to avro",
                other
            )))
        }
    })
}
