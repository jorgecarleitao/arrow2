use std::collections::BTreeMap;

use avro_rs::schema::Name;
use avro_rs::types::Value;
use avro_rs::Schema as AvroSchema;

use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// Returns the fully qualified name for a field
pub fn aliased(name: &str, namespace: Option<&str>, default_namespace: Option<&str>) -> String {
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
        AvroSchema::Record {
            doc: Some(ref doc), ..
        }
        | AvroSchema::Enum {
            doc: Some(ref doc), ..
        } => {
            props.insert("avro::doc".to_string(), doc.clone());
        }
        _ => {}
    }
    match &schema {
        AvroSchema::Record {
            name:
                Name {
                    aliases: Some(aliases),
                    namespace,
                    ..
                },
            ..
        }
        | AvroSchema::Enum {
            name:
                Name {
                    aliases: Some(aliases),
                    namespace,
                    ..
                },
            ..
        }
        | AvroSchema::Fixed {
            name:
                Name {
                    aliases: Some(aliases),
                    namespace,
                    ..
                },
            ..
        } => {
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

pub fn convert_schema(schema: &AvroSchema) -> Result<Schema> {
    let mut schema_fields = vec![];
    match schema {
        AvroSchema::Record { fields, .. } => {
            for field in fields {
                schema_fields.push(schema_to_field(
                    &field.schema,
                    Some(&field.name),
                    false,
                    Some(&external_props(&field.schema)),
                )?)
            }
        }
        schema => schema_fields.push(schema_to_field(schema, Some(""), false, None)?),
    }

    let schema = Schema::new(schema_fields);
    Ok(schema)
}

fn schema_to_field(
    schema: &AvroSchema,
    name: Option<&str>,
    mut nullable: bool,
    props: Option<&BTreeMap<String, String>>,
) -> Result<Field> {
    let data_type = match schema {
        AvroSchema::Null => DataType::Null,
        AvroSchema::Boolean => DataType::Boolean,
        AvroSchema::Int => DataType::Int32,
        AvroSchema::Long => DataType::Int64,
        AvroSchema::Float => DataType::Float32,
        AvroSchema::Double => DataType::Float64,
        AvroSchema::Bytes => DataType::Binary,
        AvroSchema::String => DataType::Utf8,
        AvroSchema::Array(item_schema) => DataType::List(Box::new(schema_to_field(
            item_schema,
            Some("item"), // default name for list items
            false,
            None,
        )?)),
        AvroSchema::Map(_) => todo!("Avro maps are mapped to MapArrays"),
        AvroSchema::Union(us) => {
            // If there are only two variants and one of them is null, set the other type as the field data type
            let has_nullable = us.find_schema(&Value::Null).is_some();
            let sub_schemas = us.variants();
            if has_nullable && sub_schemas.len() == 2 {
                nullable = true;
                if let Some(schema) = sub_schemas
                    .iter()
                    .find(|&schema| !matches!(schema, AvroSchema::Null))
                {
                    schema_to_field(schema, None, has_nullable, None)?
                        .data_type()
                        .clone()
                } else {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Can't read avro union {:?}",
                        us
                    )));
                }
            } else {
                let fields = sub_schemas
                    .iter()
                    .map(|s| schema_to_field(s, None, has_nullable, None))
                    .collect::<Result<Vec<Field>>>()?;
                DataType::Union(fields, None, false)
            }
        }
        AvroSchema::Record { name, fields, .. } => {
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
                        Some(&format!("{}.{}", name.fullname(None), field.name)),
                        false,
                        Some(&props),
                    )
                })
                .collect();
            DataType::Struct(fields?)
        }
        AvroSchema::Enum { .. } => {
            return Ok(Field::new(
                name.unwrap_or_default(),
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ))
        }
        AvroSchema::Fixed { size, .. } => DataType::FixedSizeBinary(*size),
        AvroSchema::Decimal {
            precision, scale, ..
        } => DataType::Decimal(*precision, *scale),
        AvroSchema::Uuid => DataType::Utf8,
        AvroSchema::Date => DataType::Date32,
        AvroSchema::TimeMillis => DataType::Time32(TimeUnit::Millisecond),
        AvroSchema::TimeMicros => DataType::Time64(TimeUnit::Microsecond),
        AvroSchema::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
        AvroSchema::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
        AvroSchema::Duration => DataType::Interval(IntervalUnit::MonthDayNano),
    };

    let name = name.unwrap_or_default();

    let mut field = Field::new(name, data_type, nullable);
    field.set_metadata(props.cloned());
    Ok(field)
}
