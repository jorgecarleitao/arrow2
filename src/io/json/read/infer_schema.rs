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
fn coerce_data_type(dt: Vec<&DataType>) -> Result<DataType> {
    match dt.len() {
        1 => Ok(dt[0].clone()),
        2 => {
            // there can be a case where a list and scalar both exist
            if dt.contains(&&DataType::List(Box::new(Field::new(
                "item",
                DataType::Float64,
                true,
            )))) || dt.contains(&&DataType::List(Box::new(Field::new(
                "item",
                DataType::Int64,
                true,
            )))) || dt.contains(&&DataType::List(Box::new(Field::new(
                "item",
                DataType::Boolean,
                true,
            )))) || dt.contains(&&DataType::List(Box::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))) {
                // we have a list and scalars, so we should get the values and coerce them
                let mut dt = dt;
                // sorting guarantees that the list will be the second value
                dt.sort();
                match (dt[0], dt[1]) {
                    (t1, DataType::List(e)) if e.data_type() == &DataType::Float64 => {
                        if t1 == &DataType::Float64 {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                DataType::Float64,
                                true,
                            ))))
                        } else {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                coerce_data_type(vec![t1, &DataType::Float64])?,
                                true,
                            ))))
                        }
                    }
                    (t1, DataType::List(e)) if e.data_type() == &DataType::Int64 => {
                        if t1 == &DataType::Int64 {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                DataType::Int64,
                                true,
                            ))))
                        } else {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                coerce_data_type(vec![t1, &DataType::Int64])?,
                                true,
                            ))))
                        }
                    }
                    (t1, DataType::List(e)) if e.data_type() == &DataType::Boolean => {
                        if t1 == &DataType::Boolean {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                DataType::Boolean,
                                true,
                            ))))
                        } else {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                coerce_data_type(vec![t1, &DataType::Boolean])?,
                                true,
                            ))))
                        }
                    }
                    (t1, DataType::List(e)) if e.data_type() == &DataType::Utf8 => {
                        if t1 == &DataType::Utf8 {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                DataType::Utf8,
                                true,
                            ))))
                        } else {
                            Ok(DataType::List(Box::new(Field::new(
                                "item",
                                coerce_data_type(vec![t1, &DataType::Utf8])?,
                                true,
                            ))))
                        }
                    }
                    (t1, t2) => Err(ArrowError::Schema(format!(
                        "Cannot coerce data types for {:?} and {:?}",
                        t1, t2
                    ))),
                }
            } else if dt.contains(&&DataType::Float64) && dt.contains(&&DataType::Int64) {
                Ok(DataType::Float64)
            } else {
                Ok(DataType::Utf8)
            }
        }
        _ => {
            // TODO(nevi_me) It's possible to have [float, int, list(float)], which should
            // return list(float). Will hash this out later
            Ok(DataType::List(Box::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            ))))
        }
    }
}

/// Generate schema from JSON field names and inferred data types
fn generate_schema(spec: HashMap<String, HashSet<DataType>>) -> Result<Schema> {
    let fields: Result<Vec<Field>> = spec
        .iter()
        .map(|(k, hs)| {
            let v: Vec<&DataType> = hs.iter().collect();
            coerce_data_type(v).map(|t| Field::new(k, t, true))
        })
        .collect();
    match fields {
        Ok(fields) => {
            let schema = Schema::new(fields);
            Ok(schema)
        }
        Err(e) => Err(e),
    }
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
/// use std::fs::File;
/// use std::io::{BufReader, SeekFrom, Seek};
/// use flate2::read::GzDecoder;
/// use arrow2::io::json::infer_json_schema;
///
/// let mut file = File::open("test/data/mixed_arrays.json.gz").unwrap();
///
/// // file's cursor's offset at 0
/// let mut reader = BufReader::new(GzDecoder::new(&file));
/// let inferred_schema = infer_json_schema(&mut reader, None).unwrap();
/// // cursor's offset at end of file
///
/// // seek back to start so that the original file is usable again
/// file.seek(SeekFrom::Start(0)).unwrap();
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
                                        types.into_iter().filter_map(|t| t).collect();
                                    types.dedup();
                                    // if a record contains only nulls, it is not
                                    // added to values
                                    if !types.is_empty() {
                                        let dt = coerce_data_type(types)?;

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

    generate_schema(values)
}

/// Infer the fields of a JSON file by reading the first n records of the file, with
/// `max_read_records` controlling the maximum number of records to read.
///
/// If `max_read_records` is not set, the whole file is read to infer its field types.
///
/// Contrary to [`infer_json_schema`], this function will seek back to the start of the `reader`.
/// That way, the `reader` can be used immediately afterwards to create a [`Reader`].
///
/// # Examples
/// ```
/// use std::fs::File;
/// use std::io::BufReader;
/// use arrow2::io::json::infer_json_schema_from_seekable;
///
/// let file = File::open("test/data/mixed_arrays.json").unwrap();
/// // file's cursor's offset at 0
/// let mut reader = BufReader::new(file);
/// let inferred_schema = infer_json_schema_from_seekable(&mut reader, None).unwrap();
/// // file's cursor's offset automatically set at 0
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
            coerce_data_type(vec![
                &Float64,
                &List(Box::new(Field::new("item", Float64, true)))
            ])
            .unwrap()
        );
        assert_eq!(
            List(Box::new(Field::new("item", Float64, true))),
            coerce_data_type(vec![
                &Float64,
                &List(Box::new(Field::new("item", Int64, true)))
            ])
            .unwrap()
        );
        assert_eq!(
            List(Box::new(Field::new("item", Int64, true))),
            coerce_data_type(vec![
                &Int64,
                &List(Box::new(Field::new("item", Int64, true)))
            ])
            .unwrap()
        );
        // boolean and number are incompatible, return utf8
        assert_eq!(
            List(Box::new(Field::new("item", Utf8, true))),
            coerce_data_type(vec![
                &Boolean,
                &List(Box::new(Field::new("item", Float64, true)))
            ])
            .unwrap()
        );
    }
}
