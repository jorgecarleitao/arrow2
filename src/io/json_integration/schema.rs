// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
};

use serde_derive::Deserialize;
use serde_json::{json, Value};

use crate::{
    datatypes::UnionMode,
    error::{ArrowError, Result},
};

use crate::datatypes::{
    get_extension, DataType, Field, IntegerType, IntervalUnit, Schema, TimeUnit,
};

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
            DataType::Union(_, _, _) => json!({"name": "union"}),
            DataType::Map(_, _) => json!({"name": "map"}),
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
                IntervalUnit::MonthDayNano => "MONTH_DAY_NANO",
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
            DataType::Extension(_, inner_data_type, _) => inner_data_type.to_json(),
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
            DataType::Dictionary(ref index_type, ref value_type) => {
                let index_type: DataType = (*index_type).into();
                json!({
                    "name": self.name(),
                    "nullable": self.is_nullable(),
                    "type": value_type.to_json(),
                    "children": children,
                    "dictionary": {
                        "id": self.dict_id(),
                        "indexType": index_type.to_json(),
                        "isOrdered": self.dict_is_ordered()
                    }
                })
            }
            _ => json!({
                "name": self.name(),
                "nullable": self.is_nullable(),
                "type": self.data_type().to_json(),
                "children": children
            }),
        }
    }
}

fn to_time_unit(item: Option<&Value>) -> Result<TimeUnit> {
    match item {
        Some(p) if p == "SECOND" => Ok(TimeUnit::Second),
        Some(p) if p == "MILLISECOND" => Ok(TimeUnit::Millisecond),
        Some(p) if p == "MICROSECOND" => Ok(TimeUnit::Microsecond),
        Some(p) if p == "NANOSECOND" => Ok(TimeUnit::Nanosecond),
        _ => Err(ArrowError::OutOfSpec(
            "time unit missing or invalid".to_string(),
        )),
    }
}

fn to_int(item: &Value) -> Result<IntegerType> {
    Ok(match item.get("isSigned") {
        Some(&Value::Bool(true)) => match item.get("bitWidth") {
            Some(&Value::Number(ref n)) => match n.as_u64() {
                Some(8) => IntegerType::Int8,
                Some(16) => IntegerType::Int16,
                Some(32) => IntegerType::Int32,
                Some(64) => IntegerType::Int64,
                _ => {
                    return Err(ArrowError::OutOfSpec(
                        "int bitWidth missing or invalid".to_string(),
                    ))
                }
            },
            _ => {
                return Err(ArrowError::OutOfSpec(
                    "int bitWidth missing or invalid".to_string(),
                ))
            }
        },
        Some(&Value::Bool(false)) => match item.get("bitWidth") {
            Some(&Value::Number(ref n)) => match n.as_u64() {
                Some(8) => IntegerType::UInt8,
                Some(16) => IntegerType::UInt16,
                Some(32) => IntegerType::UInt32,
                Some(64) => IntegerType::UInt64,
                _ => {
                    return Err(ArrowError::OutOfSpec(
                        "int bitWidth missing or invalid".to_string(),
                    ))
                }
            },
            _ => {
                return Err(ArrowError::OutOfSpec(
                    "int bitWidth missing or invalid".to_string(),
                ))
            }
        },
        _ => {
            return Err(ArrowError::OutOfSpec(
                "int signed missing or invalid".to_string(),
            ))
        }
    })
}

fn children(children: Option<&Value>) -> Result<Vec<Field>> {
    children
        .map(|x| {
            if let Value::Array(values) = x {
                values
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<Vec<_>>>()
            } else {
                Err(ArrowError::OutOfSpec(
                    "children must be an array".to_string(),
                ))
            }
        })
        .unwrap_or_else(|| Ok(vec![]))
}

fn read_metadata(metadata: &Value) -> Result<BTreeMap<String, String>> {
    match metadata {
        Value::Array(ref values) => {
            let mut res: BTreeMap<String, String> = BTreeMap::new();
            for value in values {
                match value.as_object() {
                    Some(map) => {
                        if map.len() != 2 {
                            return Err(ArrowError::OutOfSpec(
                                "Field 'metadata' must have exact two entries for each key-value map".to_string(),
                            ));
                        }
                        if let (Some(k), Some(v)) = (map.get("key"), map.get("value")) {
                            if let (Some(k_str), Some(v_str)) = (k.as_str(), v.as_str()) {
                                res.insert(k_str.to_string().clone(), v_str.to_string().clone());
                            } else {
                                return Err(ArrowError::OutOfSpec(
                                    "Field 'metadata' must have map value of string type"
                                        .to_string(),
                                ));
                            }
                        } else {
                            return Err(ArrowError::OutOfSpec(
                                "Field 'metadata' lacks map keys named \"key\" or \"value\""
                                    .to_string(),
                            ));
                        }
                    }
                    _ => {
                        return Err(ArrowError::OutOfSpec(
                            "Field 'metadata' contains non-object key-value pair".to_string(),
                        ));
                    }
                }
            }
            Ok(res)
        }
        Value::Object(ref values) => {
            let mut res: BTreeMap<String, String> = BTreeMap::new();
            for (k, v) in values {
                if let Some(str_value) = v.as_str() {
                    res.insert(k.clone(), str_value.to_string().clone());
                } else {
                    return Err(ArrowError::OutOfSpec(format!(
                        "Field 'metadata' contains non-string value for key {}",
                        k
                    )));
                }
            }
            Ok(res)
        }
        _ => Err(ArrowError::OutOfSpec(
            "Invalid json value type for field".to_string(),
        )),
    }
}

fn to_data_type(item: &Value, mut children: Vec<Field>) -> Result<DataType> {
    let type_ = item
        .get("name")
        .ok_or_else(|| ArrowError::OutOfSpec("type missing".to_string()))?;

    let type_ = if let Value::String(name) = type_ {
        name.as_str()
    } else {
        return Err(ArrowError::OutOfSpec("type is not a string".to_string()));
    };

    use DataType::*;
    Ok(match type_ {
        "null" => Null,
        "bool" => Boolean,
        "binary" => Binary,
        "largebinary" => LargeBinary,
        "fixedsizebinary" => {
            // return a list with any type as its child isn't defined in the map
            if let Some(Value::Number(size)) = item.get("byteWidth") {
                DataType::FixedSizeBinary(size.as_i64().unwrap() as usize)
            } else {
                return Err(ArrowError::OutOfSpec(
                    "Expecting a byteWidth for fixedsizebinary".to_string(),
                ));
            }
        }
        "utf8" => Utf8,
        "largeutf8" => LargeUtf8,
        "decimal" => {
            // return a list with any type as its child isn't defined in the map
            let precision = match item.get("precision") {
                Some(p) => Ok(p.as_u64().unwrap() as usize),
                None => Err(ArrowError::OutOfSpec(
                    "Expecting a precision for decimal".to_string(),
                )),
            };
            let scale = match item.get("scale") {
                Some(s) => Ok(s.as_u64().unwrap() as usize),
                _ => Err(ArrowError::OutOfSpec(
                    "Expecting a scale for decimal".to_string(),
                )),
            };

            DataType::Decimal(precision?, scale?)
        }
        "floatingpoint" => match item.get("precision") {
            Some(p) if p == "HALF" => DataType::Float16,
            Some(p) if p == "SINGLE" => DataType::Float32,
            Some(p) if p == "DOUBLE" => DataType::Float64,
            _ => {
                return Err(ArrowError::OutOfSpec(
                    "floatingpoint precision missing or invalid".to_string(),
                ))
            }
        },
        "timestamp" => {
            let unit = to_time_unit(item.get("unit"))?;
            let tz = match item.get("timezone") {
                None => Ok(None),
                Some(Value::String(tz)) => Ok(Some(tz.clone())),
                _ => Err(ArrowError::OutOfSpec(
                    "timezone must be a string".to_string(),
                )),
            }?;
            DataType::Timestamp(unit, tz)
        }
        "date" => match item.get("unit") {
            Some(p) if p == "DAY" => DataType::Date32,
            Some(p) if p == "MILLISECOND" => DataType::Date64,
            _ => {
                return Err(ArrowError::OutOfSpec(
                    "date unit missing or invalid".to_string(),
                ))
            }
        },
        "time" => {
            let unit = to_time_unit(item.get("unit"))?;
            match item.get("bitWidth") {
                Some(p) if p == 32 => DataType::Time32(unit),
                Some(p) if p == 64 => DataType::Time64(unit),
                _ => {
                    return Err(ArrowError::OutOfSpec(
                        "time bitWidth missing or invalid".to_string(),
                    ))
                }
            }
        }
        "duration" => {
            let unit = to_time_unit(item.get("unit"))?;
            DataType::Duration(unit)
        }
        "interval" => match item.get("unit") {
            Some(p) if p == "DAY_TIME" => DataType::Interval(IntervalUnit::DayTime),
            Some(p) if p == "YEAR_MONTH" => DataType::Interval(IntervalUnit::YearMonth),
            Some(p) if p == "MONTH_DAY_NANO" => DataType::Interval(IntervalUnit::MonthDayNano),
            _ => {
                return Err(ArrowError::OutOfSpec(
                    "interval unit missing or invalid".to_string(),
                ))
            }
        },
        "int" => to_int(item).map(|x| x.into())?,
        "list" => DataType::List(Box::new(children.pop().unwrap())),
        "largelist" => DataType::LargeList(Box::new(children.pop().unwrap())),
        "fixedsizelist" => {
            if let Some(Value::Number(size)) = item.get("listSize") {
                DataType::FixedSizeList(
                    Box::new(children.pop().unwrap()),
                    size.as_i64().unwrap() as usize,
                )
            } else {
                return Err(ArrowError::OutOfSpec(
                    "Expecting a listSize for fixedsizelist".to_string(),
                ));
            }
        }
        "struct" => DataType::Struct(children),
        "union" => {
            let mode = if let Some(Value::String(mode)) = item.get("mode") {
                UnionMode::sparse(mode == "SPARSE")
            } else {
                return Err(ArrowError::OutOfSpec("union requires mode".to_string()));
            };
            let ids = if let Some(Value::Array(ids)) = item.get("typeIds") {
                Some(ids.iter().map(|x| x.as_i64().unwrap() as i32).collect())
            } else {
                return Err(ArrowError::OutOfSpec("union requires ids".to_string()));
            };
            DataType::Union(children, ids, mode)
        }
        "map" => {
            let sorted_keys = if let Some(Value::Bool(sorted_keys)) = item.get("keysSorted") {
                *sorted_keys
            } else {
                return Err(ArrowError::OutOfSpec("sorted keys not defined".to_string()));
            };
            DataType::Map(Box::new(children.pop().unwrap()), sorted_keys)
        }
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "invalid json value type \"{}\"",
                other
            )))
        }
    })
}

impl TryFrom<&Value> for Field {
    type Error = ArrowError;

    fn try_from(value: &Value) -> Result<Self> {
        match *value {
            Value::Object(ref map) => {
                let name = match map.get("name") {
                    Some(&Value::String(ref name)) => name.to_string(),
                    _ => {
                        return Err(ArrowError::OutOfSpec(
                            "Field missing 'name' attribute".to_string(),
                        ));
                    }
                };
                let nullable = match map.get("nullable") {
                    Some(&Value::Bool(b)) => b,
                    _ => {
                        return Err(ArrowError::OutOfSpec(
                            "Field missing 'nullable' attribute".to_string(),
                        ));
                    }
                };

                let children = children(map.get("children"))?;

                let metadata = if let Some(metadata) = map.get("metadata") {
                    Some(read_metadata(metadata)?)
                } else {
                    None
                };

                let extension = get_extension(&metadata);

                let type_ = map
                    .get("type")
                    .ok_or_else(|| ArrowError::OutOfSpec("type missing".to_string()))?;

                let data_type = to_data_type(type_, children)?;

                let data_type = if let Some((name, metadata)) = extension {
                    DataType::Extension(name, Box::new(data_type), metadata)
                } else {
                    data_type
                };

                let data_type = if let Some(dictionary) = map.get("dictionary") {
                    let index_type = match dictionary.get("indexType") {
                        Some(t) => to_int(t)?,
                        _ => {
                            return Err(ArrowError::OutOfSpec(
                                "Field missing 'indexType' attribute".to_string(),
                            ));
                        }
                    };
                    DataType::Dictionary(index_type, Box::new(data_type))
                } else {
                    data_type
                };

                let (dict_id, dict_is_ordered) = if let Some(dictionary) = map.get("dictionary") {
                    let dict_id = match dictionary.get("id") {
                        Some(Value::Number(n)) => n.as_i64().unwrap(),
                        _ => {
                            return Err(ArrowError::OutOfSpec(
                                "Field missing 'id' attribute".to_string(),
                            ));
                        }
                    };
                    let dict_is_ordered = match dictionary.get("isOrdered") {
                        Some(&Value::Bool(n)) => n,
                        _ => {
                            return Err(ArrowError::OutOfSpec(
                                "Field missing 'isOrdered' attribute".to_string(),
                            ));
                        }
                    };
                    (dict_id, dict_is_ordered)
                } else {
                    (0, false)
                };
                let mut f = Field::new_dict(&name, data_type, nullable, dict_id, dict_is_ordered);
                f.set_metadata(metadata);
                Ok(f)
            }
            _ => Err(ArrowError::OutOfSpec(
                "Invalid json value type for field".to_string(),
            )),
        }
    }
}

impl ToJson for Schema {
    fn to_json(&self) -> Value {
        json!({
            "fields": self.fields.iter().map(|field| field.to_json()).collect::<Vec<Value>>(),
            "metadata": serde_json::to_value(&self.metadata).unwrap(),
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
fn from_metadata(json: &Value) -> Result<HashMap<String, String>> {
    match json {
        Value::Array(_) => {
            let mut hashmap = HashMap::new();
            let values: Vec<MetadataKeyValue> = serde_json::from_value(json.clone())?;
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
                    Err(ArrowError::OutOfSpec(
                        "metadata `value` field must be a string".to_string(),
                    ))
                }
            })
            .collect::<Result<_>>(),
        _ => Err(ArrowError::OutOfSpec(
            "`metadata` field must be an object".to_string(),
        )),
    }
}

impl TryFrom<&Value> for Schema {
    type Error = ArrowError;

    fn try_from(json: &Value) -> Result<Self> {
        match *json {
            Value::Object(ref schema) => {
                let fields = if let Some(Value::Array(fields)) = schema.get("fields") {
                    fields.iter().map(Field::try_from).collect::<Result<_>>()?
                } else {
                    return Err(ArrowError::OutOfSpec(
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
            _ => Err(ArrowError::OutOfSpec(
                "Invalid json value type for schema".to_string(),
            )),
        }
    }
}
