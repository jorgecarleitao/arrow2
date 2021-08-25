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

use std::io::{BufReader, Read, Seek, SeekFrom};

use indexmap::map::IndexMap as HashMap;
use indexmap::set::IndexSet as HashSet;
use serde_json::Value;

use super::util::ValueIter;

use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// Coerce data type during inference
///
/// * `Int64` and `Float64` should be `Float64`
/// * Lists and scalars are coerced to a list of a compatible scalar
/// * All other types are coerced to `Utf8`
fn coerce_data_type(dt: &[&DataType]) -> DataType {
    use DataType::*;
    if dt.len() == 1 {
        return dt[0].clone();
    } else if dt.len() > 2 {
        return List(Box::new(Field::new("item", Utf8, true)));
    }
    let (lhs, rhs) = (dt[0], dt[1]);

    return match (lhs, rhs) {
        (lhs, rhs) if lhs == rhs => lhs.clone(),
        (List(lhs), List(rhs)) => {
            let inner = coerce_data_type(&[lhs.data_type(), rhs.data_type()]);
            List(Box::new(Field::new("item", inner, true)))
        }
        (scalar, List(list)) => {
            let inner = coerce_data_type(&[scalar, list.data_type()]);
            List(Box::new(Field::new("item", inner, true)))
        }
        (List(list), scalar) => {
            let inner = coerce_data_type(&[scalar, list.data_type()]);
            List(Box::new(Field::new("item", inner, true)))
        }
        (Float64, Int64) => Float64,
        (Int64, Float64) => Float64,
        (Int64, Boolean) => Int64,
        (Boolean, Int64) => Int64,
        (_, _) => Utf8,
    };
}

/// Generate schema from JSON field names and inferred data types
fn generate_schema(spec: HashMap<String, HashSet<DataType>>) -> Schema {
    let fields: Vec<Field> = spec
        .iter()
        .map(|(k, hs)| {
            let v: Vec<&DataType> = hs.iter().collect();
            Field::new(k, coerce_data_type(&v), true)
        })
        .collect();
    Schema::new(fields)
}

/// Infer the fields of a JSON file by reading the first n records of the buffer, with
/// `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its field types.
///
/// This function will not seek back to the start of the `reader`. The user has to manage the
/// original file's cursor. This function is useful when the `reader`'s cursor is not available
/// (does not implement [`Seek`]), such is the case for compressed streams decoders.
///
/// # Examples
/// ```
/// use std::io::{BufReader, Cursor, SeekFrom, Seek};
/// use arrow2::io::json::infer_json_schema;
///
/// let data = r#"{"a":1, "b":[2.0, 1.3, -6.1], "c":[false, true], "d":4.1}
/// {"a":-10, "b":[2.0, 1.3, -6.1], "c":null, "d":null}
/// {"a":2, "b":[2.0, null, -6.1], "c":[false, null], "d":"text"}
/// {"a":3, "b":4, "c": true, "d":[1, false, "array", 2.4]}
/// "#;
///
/// // file's cursor's offset at 0
/// let mut reader = BufReader::new(Cursor::new(data));
/// let inferred_schema = infer_json_schema(&mut reader, None).unwrap();
/// ```
pub fn infer_json_schema<R: Read>(
    reader: &mut BufReader<R>,
    max_read_records: Option<usize>,
) -> Result<Schema> {
    infer_json_schema_from_iterator(ValueIter::new(reader, max_read_records))
}

/// Infer the fields of a JSON file by reading all items from the JSON Value Iterator.
pub fn infer_json_schema_from_iterator<I>(value_iter: I) -> Result<Schema>
where
    I: Iterator<Item = Result<Value>>,
{
    let mut values: HashMap<String, HashSet<DataType>> = HashMap::new();

    for record in value_iter {
        match record? {
            Value::Object(map) => {
                let res = map.iter().try_for_each(|(k, v)| {
                    match v {
                        Value::Array(a) => {
                            // collect the data types in array
                            let types: Result<Vec<Option<&DataType>>> = a
                                .iter()
                                .map(|a| match a {
                                    Value::Null => Ok(None),
                                    Value::Number(n) => {
                                        if n.is_i64() {
                                            Ok(Some(&DataType::Int64))
                                        } else {
                                            Ok(Some(&DataType::Float64))
                                        }
                                    }
                                    Value::Bool(_) => Ok(Some(&DataType::Boolean)),
                                    Value::String(_) => Ok(Some(&DataType::Utf8)),
                                    Value::Array(_) | Value::Object(_) => {
                                        Err(ArrowError::NotYetImplemented(
                                            "Nested lists and structs not supported".to_string(),
                                        ))
                                    }
                                })
                                .collect();
                            match types {
                                Ok(types) => {
                                    // unwrap the Option and discard None values (from
                                    // JSON nulls)
                                    let mut types: Vec<&DataType> =
                                        types.into_iter().flatten().collect();
                                    types.dedup();
                                    // if a record contains only nulls, it is not
                                    // added to values
                                    if !types.is_empty() {
                                        let dt = coerce_data_type(&types);

                                        if values.contains_key(k) {
                                            let x = values.get_mut(k).unwrap();
                                            x.insert(DataType::List(Box::new(Field::new(
                                                "item", dt, true,
                                            ))));
                                        } else {
                                            // create hashset and add value type
                                            let mut hs = HashSet::new();
                                            hs.insert(DataType::List(Box::new(Field::new(
                                                "item", dt, true,
                                            ))));
                                            values.insert(k.to_string(), hs);
                                        }
                                    }
                                    Ok(())
                                }
                                Err(e) => Err(e),
                            }
                        }
                        Value::Bool(_) => {
                            if values.contains_key(k) {
                                let x = values.get_mut(k).unwrap();
                                x.insert(DataType::Boolean);
                            } else {
                                // create hashset and add value type
                                let mut hs = HashSet::new();
                                hs.insert(DataType::Boolean);
                                values.insert(k.to_string(), hs);
                            }
                            Ok(())
                        }
                        Value::Null => {
                            // do nothing, we treat json as nullable by default when
                            // inferring
                            Ok(())
                        }
                        Value::Number(n) => {
                            if n.is_f64() {
                                if values.contains_key(k) {
                                    let x = values.get_mut(k).unwrap();
                                    x.insert(DataType::Float64);
                                } else {
                                    // create hashset and add value type
                                    let mut hs = HashSet::new();
                                    hs.insert(DataType::Float64);
                                    values.insert(k.to_string(), hs);
                                }
                            } else {
                                // default to i64
                                if values.contains_key(k) {
                                    let x = values.get_mut(k).unwrap();
                                    x.insert(DataType::Int64);
                                } else {
                                    // create hashset and add value type
                                    let mut hs = HashSet::new();
                                    hs.insert(DataType::Int64);
                                    values.insert(k.to_string(), hs);
                                }
                            }
                            Ok(())
                        }
                        Value::String(_) => {
                            if values.contains_key(k) {
                                let x = values.get_mut(k).unwrap();
                                x.insert(DataType::Utf8);
                            } else {
                                // create hashset and add value type
                                let mut hs = HashSet::new();
                                hs.insert(DataType::Utf8);
                                values.insert(k.to_string(), hs);
                            }
                            Ok(())
                        }
                        Value::Object(_) => Err(ArrowError::NotYetImplemented(
                            "Inferring schema from nested JSON structs currently not supported"
                                .to_string(),
                        )),
                    }
                });
                match res {
                    Ok(()) => {}
                    Err(e) => return Err(e),
                }
            }
            value => {
                return Err(ArrowError::Other(format!(
                    "Expected JSON record to be an object, found {:?}",
                    value
                )));
            }
        };
    }

    Ok(generate_schema(values))
}

/// Infer the fields of a JSON file by reading the first n records of the file, with
/// `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its field types.
///
/// Contrary to [`infer_json_schema`], this function will seek back to the start of the `reader`.
/// That way, the `reader` can be used immediately afterwards.
///
/// # Examples
/// ```
/// use std::fs::File;
/// use std::io::{BufReader, Cursor};
/// use arrow2::io::json::infer_json_schema_from_seekable;
///
/// let data = r#"{"a":1, "b":[2.0, 1.3, -6.1], "c":[false, true], "d":4.1}
/// {"a":-10, "b":[2.0, 1.3, -6.1], "c":null, "d":null}
/// {"a":2, "b":[2.0, null, -6.1], "c":[false, null], "d":"text"}
/// {"a":3, "b":4, "c": true, "d":[1, false, "array", 2.4]}
/// "#;
/// let mut reader = BufReader::new(Cursor::new(data));
/// let inferred_schema = infer_json_schema_from_seekable(&mut reader, None).unwrap();
/// // cursor's position automatically set at 0
/// ```
pub fn infer_json_schema_from_seekable<R: Read + Seek>(
    reader: &mut BufReader<R>,
    max_read_records: Option<usize>,
) -> Result<Schema> {
    let schema = infer_json_schema(reader, max_read_records);
    // return the reader seek back to the start
    reader.seek(SeekFrom::Start(0))?;

    schema
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_coersion_scalar_and_list() {
        use crate::datatypes::DataType::*;

        assert_eq!(
            List(Box::new(Field::new("item", Float64, true))),
            coerce_data_type(&[&Float64, &List(Box::new(Field::new("item", Float64, true)))])
        );
        assert_eq!(
            List(Box::new(Field::new("item", Float64, true))),
            coerce_data_type(&[&Float64, &List(Box::new(Field::new("item", Int64, true)))])
        );
        assert_eq!(
            List(Box::new(Field::new("item", Int64, true))),
            coerce_data_type(&[&Int64, &List(Box::new(Field::new("item", Int64, true)))])
        );
        // boolean and number are incompatible, return utf8
        assert_eq!(
            List(Box::new(Field::new("item", Utf8, true))),
            coerce_data_type(&[&Boolean, &List(Box::new(Field::new("item", Float64, true)))])
        );
    }
}
