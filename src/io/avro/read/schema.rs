use std::collections::BTreeMap;

use avro_schema::{Enum, Fixed, Record, Schema as AvroSchema};

use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// Returns the fully qualified name for a field
fn aliased(name: &str, namespace: Option<&str>, default_namespace: Option<&str>) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        let namespace = namespace.as_ref().copied().or(default_namespace);

        match namespace {
            Some(ref namespace) => format!("{}.{}", namespace, name),
            None => name.to_string(),
        }
    }
}

fn external_props(schema: &AvroSchema) -> BTreeMap<String, String> {
    let mut props = BTreeMap::new();
    match &schema {
        AvroSchema::Record(Record {
            doc: Some(ref doc), ..
        })
        | AvroSchema::Enum(Enum {
            doc: Some(ref doc), ..
        }) => {
            props.insert("avro::doc".to_string(), doc.clone());
        }
        _ => {}
    }
    match &schema {
        AvroSchema::Record(Record {
            aliases, namespace, ..
        })
        | AvroSchema::Enum(Enum {
            aliases, namespace, ..
        })
        | AvroSchema::Fixed(Fixed {
            aliases, namespace, ..
        }) => {
            let aliases: Vec<String> = aliases
                .iter()
                .map(|alias| aliased(alias, namespace.as_deref(), None))
                .collect();
            props.insert(
                "avro::aliases".to_string(),
                format!("[{}]", aliases.join(",")),
            );
        }
        _ => {}
    }
    props
}

/// Maps an Avro Schema into a [`Schema`].
pub fn convert_schema(schema: &AvroSchema) -> Result<Schema> {
    let mut schema_fields = vec![];
    match schema {
        AvroSchema::Record(Record { fields, .. }) => {
            for field in fields {
                schema_fields.push(schema_to_field(
                    &field.schema,
                    Some(&field.name),
                    false,
                    Some(external_props(&field.schema)),
                )?)
            }
        }
        other => {
            return Err(ArrowError::OutOfSpec(format!(
                "An avro Schema must be of type Record - it is of type {:?}",
                other
            )))
        }
    };
    Ok(Schema::new(schema_fields))
}

fn schema_to_field(
    schema: &AvroSchema,
    name: Option<&str>,
    mut nullable: bool,
    props: Option<BTreeMap<String, String>>,
) -> Result<Field> {
    let data_type = match schema {
        AvroSchema::Null => DataType::Null,
        AvroSchema::Boolean => DataType::Boolean,
        AvroSchema::Int(logical) => match logical {
            Some(logical) => match logical {
                avro_schema::IntLogical::Date => DataType::Date32,
                avro_schema::IntLogical::Time => DataType::Time32(TimeUnit::Millisecond),
            },
            None => DataType::Int32,
        },
        AvroSchema::Long(logical) => match logical {
            Some(logical) => match logical {
                avro_schema::LongLogical::Time => DataType::Time64(TimeUnit::Microsecond),
                avro_schema::LongLogical::TimestampMillis => {
                    DataType::Timestamp(TimeUnit::Millisecond, Some("00:00".to_string()))
                }
                avro_schema::LongLogical::TimestampMicros => {
                    DataType::Timestamp(TimeUnit::Microsecond, Some("00:00".to_string()))
                }
                avro_schema::LongLogical::LocalTimestampMillis => {
                    DataType::Timestamp(TimeUnit::Millisecond, None)
                }
                avro_schema::LongLogical::LocalTimestampMicros => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
            },
            None => DataType::Int64,
        },
        AvroSchema::Float => DataType::Float32,
        AvroSchema::Double => DataType::Float64,
        AvroSchema::Bytes(logical) => match logical {
            Some(logical) => match logical {
                avro_schema::BytesLogical::Decimal(precision, scale) => {
                    DataType::Decimal(*precision, *scale)
                }
            },
            None => DataType::Binary,
        },
        AvroSchema::String(_) => DataType::Utf8,
        AvroSchema::Array(item_schema) => DataType::List(Box::new(schema_to_field(
            item_schema,
            Some("item"), // default name for list items
            false,
            None,
        )?)),
        AvroSchema::Map(_) => todo!("Avro maps are mapped to MapArrays"),
        AvroSchema::Union(schemas) => {
            // If there are only two variants and one of them is null, set the other type as the field data type
            let has_nullable = schemas.iter().any(|x| x == &AvroSchema::Null);
            if has_nullable && schemas.len() == 2 {
                nullable = true;
                if let Some(schema) = schemas
                    .iter()
                    .find(|&schema| !matches!(schema, AvroSchema::Null))
                {
                    schema_to_field(schema, None, has_nullable, None)?
                        .data_type()
                        .clone()
                } else {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Can't read avro union {:?}",
                        schema
                    )));
                }
            } else {
                let fields = schemas
                    .iter()
                    .map(|s| schema_to_field(s, None, has_nullable, None))
                    .collect::<Result<Vec<Field>>>()?;
                DataType::Union(fields, None, UnionMode::Dense)
            }
        }
        AvroSchema::Record(Record { name, fields, .. }) => {
            let fields: Result<Vec<Field>> = fields
                .iter()
                .map(|field| {
                    let mut props = BTreeMap::new();
                    if let Some(doc) = &field.doc {
                        props.insert("avro::doc".to_string(), doc.clone());
                    }
                    /*if let Some(aliases) = fields.aliases {
                        props.insert("aliases", aliases);
                    }*/
                    schema_to_field(
                        &field.schema,
                        Some(&format!("{}.{}", name, field.name)),
                        false,
                        Some(props),
                    )
                })
                .collect();
            DataType::Struct(fields?)
        }
        AvroSchema::Enum { .. } => {
            return Ok(Field::new(
                name.unwrap_or_default(),
                DataType::Dictionary(IntegerType::Int32, Box::new(DataType::Utf8), false),
                false,
            ))
        }
        AvroSchema::Fixed(Fixed { size, logical, .. }) => match logical {
            Some(logical) => match logical {
                avro_schema::FixedLogical::Decimal(precision, scale) => {
                    DataType::Decimal(*precision, *scale)
                }
                avro_schema::FixedLogical::Duration => {
                    DataType::Interval(IntervalUnit::MonthDayNano)
                }
            },
            None => DataType::FixedSizeBinary(*size),
        },
    };

    let name = name.unwrap_or_default();

    let mut field = Field::new(name, data_type, nullable);
    field.set_metadata(props);
    Ok(field)
}
