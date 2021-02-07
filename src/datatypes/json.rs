use std::{collections::{BTreeMap, HashMap}, convert::TryFrom};

use serde_derive::Deserialize;
use serde_json::{Value, json};

use crate::error::ArrowError;

use super::{DataType, Field, IntervalUnit, Schema, TimeUnit};

pub trait ToJson {
    /// Generate a JSON representation
    fn to_json(&self) -> Value;
}

impl ToJson for DataType {
    fn to_json(&self) -> Value {
        match self {
            DataType::Null => json!({"name": "null"}),
            DataType::Boolean => json!({"name": "bool"}),
            DataType::Int8 => json!({"name": "int", "bitWidth": 8, "isSigned": true}),
            DataType::Int16 => json!({"name": "int", "bitWidth": 16, "isSigned": true}),
            DataType::Int32 => json!({"name": "int", "bitWidth": 32, "isSigned": true}),
            DataType::Int64 => json!({"name": "int", "bitWidth": 64, "isSigned": true}),
            DataType::UInt8 => json!({"name": "int", "bitWidth": 8, "isSigned": false}),
            DataType::UInt16 => json!({"name": "int", "bitWidth": 16, "isSigned": false}),
            DataType::UInt32 => json!({"name": "int", "bitWidth": 32, "isSigned": false}),
            DataType::UInt64 => json!({"name": "int", "bitWidth": 64, "isSigned": false}),
            DataType::Float16 => json!({"name": "floatingpoint", "precision": "HALF"}),
            DataType::Float32 => json!({"name": "floatingpoint", "precision": "SINGLE"}),
            DataType::Float64 => json!({"name": "floatingpoint", "precision": "DOUBLE"}),
            DataType::Utf8 => json!({"name": "utf8"}),
            DataType::LargeUtf8 => json!({"name": "largeutf8"}),
            DataType::Binary => json!({"name": "binary"}),
            DataType::LargeBinary => json!({"name": "largebinary"}),
            DataType::FixedSizeBinary(byte_width) => {
                json!({"name": "fixedsizebinary", "byteWidth": byte_width})
            }
            DataType::Struct(_) => json!({"name": "struct"}),
            DataType::Union(_) => json!({"name": "union"}),
            DataType::List(_) => json!({ "name": "list"}),
            DataType::LargeList(_) => json!({ "name": "largelist"}),
            DataType::FixedSizeList(_, length) => {
                json!({"name":"fixedsizelist", "listSize": length})
            }
            DataType::Time32(unit) => {
                json!({"name": "time", "bitWidth": 32, "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }})
            }
            DataType::Time64(unit) => {
                json!({"name": "time", "bitWidth": 64, "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }})
            }
            DataType::Date32 => {
                json!({"name": "date", "unit": "DAY"})
            }
            DataType::Date64 => {
                json!({"name": "date", "unit": "MILLISECOND"})
            }
            DataType::Timestamp(unit, None) => {
                json!({"name": "timestamp", "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }})
            }
            DataType::Timestamp(unit, Some(tz)) => {
                json!({"name": "timestamp", "unit": match unit {
                    TimeUnit::Second => "SECOND",
                    TimeUnit::Millisecond => "MILLISECOND",
                    TimeUnit::Microsecond => "MICROSECOND",
                    TimeUnit::Nanosecond => "NANOSECOND",
                }, "timezone": tz})
            }
            DataType::Interval(unit) => json!({"name": "interval", "unit": match unit {
                IntervalUnit::YearMonth => "YEAR_MONTH",
                IntervalUnit::DayTime => "DAY_TIME",
            }}),
            DataType::Duration(unit) => json!({"name": "duration", "unit": match unit {
                TimeUnit::Second => "SECOND",
                TimeUnit::Millisecond => "MILLISECOND",
                TimeUnit::Microsecond => "MICROSECOND",
                TimeUnit::Nanosecond => "NANOSECOND",
            }}),
            DataType::Dictionary(_, _) => json!({ "name": "dictionary"}),
            DataType::Decimal(precision, scale) => {
                json!({"name": "decimal", "precision": precision, "scale": scale})
            }
        }
    }
}

impl ToJson for Field {
    fn to_json(&self) -> Value {
        let children: Vec<Value> = match self.data_type() {
            DataType::Struct(fields) => fields.iter().map(|f| f.to_json()).collect(),
            DataType::List(field) => vec![field.to_json()],
            DataType::LargeList(field) => vec![field.to_json()],
            DataType::FixedSizeList(field, _) => vec![field.to_json()],
            _ => vec![],
        };
        match self.data_type() {
            DataType::Dictionary(ref index_type, ref value_type) => json!({
                "name": self.name(),
                "nullable": self.is_nullable(),
                "type": value_type.to_json(),
                "children": children,
                "dictionary": {
                    "id": self.dict_id(),
                    "indexType": index_type.to_json(),
                    "isOrdered": self.dict_is_ordered()
                }
            }),
            _ => json!({
                "name": self.name(),
                "nullable": self.is_nullable(),
                "type": self.data_type().to_json(),
                "children": children
            }),
        }
    }
}

impl TryFrom<&Value> for DataType {
    type Error = ArrowError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let default_field = Field::new("", DataType::Boolean, true);
        match *value {
            Value::Object(ref map) => match map.get("name") {
                Some(s) if s == "null" => Ok(DataType::Null),
                Some(s) if s == "bool" => Ok(DataType::Boolean),
                Some(s) if s == "binary" => Ok(DataType::Binary),
                Some(s) if s == "largebinary" => Ok(DataType::LargeBinary),
                Some(s) if s == "utf8" => Ok(DataType::Utf8),
                Some(s) if s == "largeutf8" => Ok(DataType::LargeUtf8),
                Some(s) if s == "fixedsizebinary" => {
                    // return a list with any type as its child isn't defined in the map
                    if let Some(Value::Number(size)) = map.get("byteWidth") {
                        Ok(DataType::FixedSizeBinary(size.as_i64().unwrap() as i32))
                    } else {
                        Err(ArrowError::ParseError(
                            "Expecting a byteWidth for fixedsizebinary".to_string(),
                        ))
                    }
                }
                Some(s) if s == "decimal" => {
                    // return a list with any type as its child isn't defined in the map
                    let precision = match map.get("precision") {
                        Some(p) => Ok(p.as_u64().unwrap() as usize),
                        None => Err(ArrowError::ParseError(
                            "Expecting a precision for decimal".to_string(),
                        )),
                    };
                    let scale = match map.get("scale") {
                        Some(s) => Ok(s.as_u64().unwrap() as usize),
                        _ => Err(ArrowError::ParseError(
                            "Expecting a scale for decimal".to_string(),
                        )),
                    };

                    Ok(DataType::Decimal(precision?, scale?))
                }
                Some(s) if s == "floatingpoint" => match map.get("precision") {
                    Some(p) if p == "HALF" => Ok(DataType::Float16),
                    Some(p) if p == "SINGLE" => Ok(DataType::Float32),
                    Some(p) if p == "DOUBLE" => Ok(DataType::Float64),
                    _ => Err(ArrowError::ParseError(
                        "floatingpoint precision missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "timestamp" => {
                    let unit = match map.get("unit") {
                        Some(p) if p == "SECOND" => Ok(TimeUnit::Second),
                        Some(p) if p == "MILLISECOND" => Ok(TimeUnit::Millisecond),
                        Some(p) if p == "MICROSECOND" => Ok(TimeUnit::Microsecond),
                        Some(p) if p == "NANOSECOND" => Ok(TimeUnit::Nanosecond),
                        _ => Err(ArrowError::ParseError(
                            "timestamp unit missing or invalid".to_string(),
                        )),
                    };
                    let tz = match map.get("timezone") {
                        None => Ok(None),
                        Some(Value::String(tz)) => Ok(Some(tz.clone())),
                        _ => Err(ArrowError::ParseError(
                            "timezone must be a string".to_string(),
                        )),
                    };
                    Ok(DataType::Timestamp(unit?, tz?))
                }
                Some(s) if s == "date" => match map.get("unit") {
                    Some(p) if p == "DAY" => Ok(DataType::Date32),
                    Some(p) if p == "MILLISECOND" => Ok(DataType::Date64),
                    _ => Err(ArrowError::ParseError(
                        "date unit missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "time" => {
                    let unit = match map.get("unit") {
                        Some(p) if p == "SECOND" => Ok(TimeUnit::Second),
                        Some(p) if p == "MILLISECOND" => Ok(TimeUnit::Millisecond),
                        Some(p) if p == "MICROSECOND" => Ok(TimeUnit::Microsecond),
                        Some(p) if p == "NANOSECOND" => Ok(TimeUnit::Nanosecond),
                        _ => Err(ArrowError::ParseError(
                            "time unit missing or invalid".to_string(),
                        )),
                    };
                    match map.get("bitWidth") {
                        Some(p) if p == 32 => Ok(DataType::Time32(unit?)),
                        Some(p) if p == 64 => Ok(DataType::Time64(unit?)),
                        _ => Err(ArrowError::ParseError(
                            "time bitWidth missing or invalid".to_string(),
                        )),
                    }
                }
                Some(s) if s == "duration" => match map.get("unit") {
                    Some(p) if p == "SECOND" => Ok(DataType::Duration(TimeUnit::Second)),
                    Some(p) if p == "MILLISECOND" => {
                        Ok(DataType::Duration(TimeUnit::Millisecond))
                    }
                    Some(p) if p == "MICROSECOND" => {
                        Ok(DataType::Duration(TimeUnit::Microsecond))
                    }
                    Some(p) if p == "NANOSECOND" => {
                        Ok(DataType::Duration(TimeUnit::Nanosecond))
                    }
                    _ => Err(ArrowError::ParseError(
                        "time unit missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "interval" => match map.get("unit") {
                    Some(p) if p == "DAY_TIME" => {
                        Ok(DataType::Interval(IntervalUnit::DayTime))
                    }
                    Some(p) if p == "YEAR_MONTH" => {
                        Ok(DataType::Interval(IntervalUnit::YearMonth))
                    }
                    _ => Err(ArrowError::ParseError(
                        "interval unit missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "int" => match map.get("isSigned") {
                    Some(&Value::Bool(true)) => match map.get("bitWidth") {
                        Some(&Value::Number(ref n)) => match n.as_u64() {
                            Some(8) => Ok(DataType::Int8),
                            Some(16) => Ok(DataType::Int16),
                            Some(32) => Ok(DataType::Int32),
                            Some(64) => Ok(DataType::Int64),
                            _ => Err(ArrowError::ParseError(
                                "int bitWidth missing or invalid".to_string(),
                            )),
                        },
                        _ => Err(ArrowError::ParseError(
                            "int bitWidth missing or invalid".to_string(),
                        )),
                    },
                    Some(&Value::Bool(false)) => match map.get("bitWidth") {
                        Some(&Value::Number(ref n)) => match n.as_u64() {
                            Some(8) => Ok(DataType::UInt8),
                            Some(16) => Ok(DataType::UInt16),
                            Some(32) => Ok(DataType::UInt32),
                            Some(64) => Ok(DataType::UInt64),
                            _ => Err(ArrowError::ParseError(
                                "int bitWidth missing or invalid".to_string(),
                            )),
                        },
                        _ => Err(ArrowError::ParseError(
                            "int bitWidth missing or invalid".to_string(),
                        )),
                    },
                    _ => Err(ArrowError::ParseError(
                        "int signed missing or invalid".to_string(),
                    )),
                },
                Some(s) if s == "list" => {
                    // return a list with any type as its child isn't defined in the map
                    Ok(DataType::List(Box::new(default_field)))
                }
                Some(s) if s == "largelist" => {
                    // return a largelist with any type as its child isn't defined in the map
                    Ok(DataType::LargeList(Box::new(default_field)))
                }
                Some(s) if s == "fixedsizelist" => {
                    // return a list with any type as its child isn't defined in the map
                    if let Some(Value::Number(size)) = map.get("listSize") {
                        Ok(DataType::FixedSizeList(
                            Box::new(default_field),
                            size.as_i64().unwrap() as i32,
                        ))
                    } else {
                        Err(ArrowError::ParseError(
                            "Expecting a listSize for fixedsizelist".to_string(),
                        ))
                    }
                }
                Some(s) if s == "struct" => {
                    // return an empty `struct` type as its children aren't defined in the map
                    Ok(DataType::Struct(vec![]))
                }
                Some(other) => Err(ArrowError::ParseError(format!(
                    "invalid or unsupported type name: {} in {:?}",
                    other, value
                ))),
                None => Err(ArrowError::ParseError("type name missing".to_string())),
            },
            _ => Err(ArrowError::ParseError(
                "invalid json value type".to_string(),
            )),
        }
    }
}

impl TryFrom<&Value> for Field {
    type Error = ArrowError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match *value {
            Value::Object(ref map) => {
                let name = match map.get("name") {
                    Some(&Value::String(ref name)) => name.to_string(),
                    _ => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'name' attribute".to_string(),
                        ));
                    }
                };
                let nullable = match map.get("nullable") {
                    Some(&Value::Bool(b)) => b,
                    _ => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'nullable' attribute".to_string(),
                        ));
                    }
                };
                let data_type = match map.get("type") {
                    Some(t) => DataType::try_from(t)?,
                    _ => {
                        return Err(ArrowError::ParseError(
                            "Field missing 'type' attribute".to_string(),
                        ));
                    }
                };

                // Referenced example file: testing/data/arrow-ipc-stream/integration/1.0.0-littleendian/generated_custom_metadata.json.gz
                let metadata = match map.get("metadata") {
                    Some(&Value::Array(ref values)) => {
                        let mut res: BTreeMap<String, String> = BTreeMap::new();
                        for value in values {
                            match value.as_object() {
                                Some(map) => {
                                    if map.len() != 2 {
                                        return Err(ArrowError::ParseError(
                                            "Field 'metadata' must have exact two entries for each key-value map".to_string(),
                                        ));
                                    }
                                    if let (Some(k), Some(v)) =
                                        (map.get("key"), map.get("value"))
                                    {
                                        if let (Some(k_str), Some(v_str)) =
                                            (k.as_str(), v.as_str())
                                        {
                                            res.insert(
                                                k_str.to_string().clone(),
                                                v_str.to_string().clone(),
                                            );
                                        } else {
                                            return Err(ArrowError::ParseError("Field 'metadata' must have map value of string type".to_string()));
                                        }
                                    } else {
                                        return Err(ArrowError::ParseError("Field 'metadata' lacks map keys named \"key\" or \"value\"".to_string()));
                                    }
                                }
                                _ => {
                                    return Err(ArrowError::ParseError(
                                        "Field 'metadata' contains non-object key-value pair".to_string(),
                                    ));
                                }
                            }
                        }
                        Some(res)
                    }
                    // We also support map format, because Schema's metadata supports this.
                    // See https://github.com/apache/arrow/pull/5907
                    Some(&Value::Object(ref values)) => {
                        let mut res: BTreeMap<String, String> = BTreeMap::new();
                        for (k, v) in values {
                            if let Some(str_value) = v.as_str() {
                                res.insert(k.clone(), str_value.to_string().clone());
                            } else {
                                return Err(ArrowError::ParseError(
                                    format!("Field 'metadata' contains non-string value for key {}", k),
                                ));
                            }
                        }
                        Some(res)
                    }
                    Some(_) => {
                        return Err(ArrowError::ParseError(
                            "Field `metadata` is not json array".to_string(),
                        ));
                    }
                    _ => None,
                };

                // if data_type is a struct or list, get its children
                let data_type = match data_type {
                    DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _) => match map.get("children") {
                        Some(Value::Array(values)) => {
                            if values.len() != 1 {
                                return Err(ArrowError::ParseError(
                                    "Field 'children' must have one element for a list data type".to_string(),
                                ));
                            }
                            match data_type {
                                    DataType::List(_) => {
                                        DataType::List(Box::new(Self::try_from(&values[0])?))
                                    }
                                    DataType::LargeList(_) => {
                                        DataType::LargeList(Box::new(Self::try_from(&values[0])?))
                                    }
                                    DataType::FixedSizeList(_, int) => DataType::FixedSizeList(
                                        Box::new(Self::try_from(&values[0])?),
                                        int,
                                    ),
                                    _ => unreachable!(
                                        "Data type should be a list, largelist or fixedsizelist"
                                    ),
                                }
                        }
                        Some(_) => {
                            return Err(ArrowError::ParseError(
                                "Field 'children' must be an array".to_string(),
                            ))
                        }
                        None => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'children' attribute".to_string(),
                            ));
                        }
                    },
                    DataType::Struct(mut fields) => match map.get("children") {
                        Some(Value::Array(values)) => {
                            let struct_fields: Result<Vec<Field>, _> =
                                values.iter().map(|v| Field::try_from(v)).collect();
                            fields.append(&mut struct_fields?);
                            DataType::Struct(fields)
                        }
                        Some(_) => {
                            return Err(ArrowError::ParseError(
                                "Field 'children' must be an array".to_string(),
                            ))
                        }
                        None => {
                            return Err(ArrowError::ParseError(
                                "Field missing 'children' attribute".to_string(),
                            ));
                        }
                    },
                    _ => data_type,
                };

                let mut dict_id = 0;
                let mut dict_is_ordered = false;

                let data_type = match map.get("dictionary") {
                    Some(dictionary) => {
                        let index_type = match dictionary.get("indexType") {
                            Some(t) => DataType::try_from(t)?,
                            _ => {
                                return Err(ArrowError::ParseError(
                                    "Field missing 'indexType' attribute".to_string(),
                                ));
                            }
                        };
                        dict_id = match dictionary.get("id") {
                            Some(Value::Number(n)) => n.as_i64().unwrap(),
                            _ => {
                                return Err(ArrowError::ParseError(
                                    "Field missing 'id' attribute".to_string(),
                                ));
                            }
                        };
                        dict_is_ordered = match dictionary.get("isOrdered") {
                            Some(&Value::Bool(n)) => n,
                            _ => {
                                return Err(ArrowError::ParseError(
                                    "Field missing 'isOrdered' attribute".to_string(),
                                ));
                            }
                        };
                        DataType::Dictionary(Box::new(index_type), Box::new(data_type))
                    }
                    _ => data_type,
                };
                Ok(Field {
                    name,
                    nullable,
                    data_type,
                    dict_id,
                    dict_is_ordered,
                    metadata,
                })
            }
            _ => Err(ArrowError::ParseError(
                "Invalid json value type for field".to_string(),
            )),
        }
    }
}

impl ToJson for Schema {
    fn to_json(&self) -> Value {
        json!({
            "fields": self.fields.iter().map(|field| field.to_json()).collect::<Vec<Value>>(),
            "metadata": serde_json::to_value(&self.metadata).unwrap()
        })
    }
}

#[derive(Deserialize)]
struct MetadataKeyValue {
    key: String,
    value: String,
}

/// Parse a `metadata` definition from a JSON representation.
/// The JSON can either be an Object or an Array of Objects.
fn from_metadata(json: &Value) -> Result<HashMap<String, String>, ArrowError> {
    match json {
        Value::Array(_) => {
            let mut hashmap = HashMap::new();
            let values: Vec<MetadataKeyValue> = serde_json::from_value(json.clone())
                .map_err(|_| {
                    ArrowError::JsonError(
                        "Unable to parse object into key-value pair".to_string(),
                    )
                })?;
            for meta in values {
                hashmap.insert(meta.key.clone(), meta.value);
            }
            Ok(hashmap)
        }
        Value::Object(md) => md
            .iter()
            .map(|(k, v)| {
                if let Value::String(v) = v {
                    Ok((k.to_string(), v.to_string()))
                } else {
                    Err(ArrowError::ParseError(
                        "metadata `value` field must be a string".to_string(),
                    ))
                }
            })
            .collect::<Result<_, _>>(),
        _ => Err(ArrowError::ParseError(
            "`metadata` field must be an object".to_string(),
        )),
    }
}

impl TryFrom<&Value> for Schema {
    type Error = ArrowError;

    fn try_from(json: &Value) -> Result<Self, Self::Error> {
        match *json {
            Value::Object(ref schema) => {
                let fields = if let Some(Value::Array(fields)) = schema.get("fields") {
                    fields
                        .iter()
                        .map(|f| Field::try_from(f))
                        .collect::<Result<_, _>>()?
                } else {
                    return Err(ArrowError::ParseError(
                        "Schema fields should be an array".to_string(),
                    ));
                };

                let metadata = if let Some(value) = schema.get("metadata") {
                    from_metadata(value)?
                } else {
                    HashMap::default()
                };

                Ok(Self { fields, metadata })
            }
            _ => Err(ArrowError::ParseError(
                "Invalid json value type for schema".to_string(),
            )),
        }
    }
}
