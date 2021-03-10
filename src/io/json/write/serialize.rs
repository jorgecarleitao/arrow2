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

use serde_json::map::Map;
use serde_json::{Number, Value};

use crate::{array::*, datatypes::*, record_batch::RecordBatch, types::NativeType};

trait JsonSerializable {
    fn into_json_value(self) -> Option<Value>;
}

impl JsonSerializable for i8 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl JsonSerializable for i16 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl JsonSerializable for i32 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl JsonSerializable for i64 {
    fn into_json_value(self) -> Option<Value> {
        Some(Value::Number(Number::from(self)))
    }
}

impl JsonSerializable for u8 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl JsonSerializable for u16 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl JsonSerializable for u32 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl JsonSerializable for u64 {
    fn into_json_value(self) -> Option<Value> {
        Some(self.into())
    }
}

impl JsonSerializable for f32 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(f64::round(self as f64 * 1000.0) / 1000.0).map(Value::Number)
    }
}

impl JsonSerializable for f64 {
    fn into_json_value(self) -> Option<Value> {
        Number::from_f64(self).map(Value::Number)
    }
}

#[inline]
fn to_json<T: NativeType>(value: Option<T>) -> Value
where
    T: NativeType + JsonSerializable,
{
    value
        .and_then(|x| x.into_json_value())
        .unwrap_or(Value::Null)
}

fn primitive_array_to_json<T>(array: &dyn Array) -> Vec<Value>
where
    T: NativeType + JsonSerializable,
{
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    array.iter().map(to_json).collect()
}

fn struct_array_to_jsonmap_array(array: &StructArray, row_count: usize) -> Vec<Map<String, Value>> {
    // {"a": [1, 2, 3], "b": [a, b, c], "c": {"a": [1, 2, 3]}}
    // [
    //  {"a": 1, "b": a, "c": {"a": 1}},
    //  {"a": 2, "b": b, "c": {"a": 2}},
    //  {"a": 3, "b": c, "c": {"a": 3}},
    // ]
    //

    let fields = array.fields();

    let mut inner_objs = std::iter::repeat(Map::new())
        .take(row_count)
        .collect::<Vec<Map<String, Value>>>();

    // todo: use validity...
    array
        .values()
        .iter()
        .enumerate()
        .for_each(|(j, struct_col)| {
            set_column_for_json_rows(
                &mut inner_objs,
                row_count,
                struct_col.as_ref(),
                fields[j].name(),
            );
        });

    inner_objs
}

fn write_array(array: &dyn Array) -> Value {
    Value::Array(match array.data_type() {
        DataType::Null => std::iter::repeat(Value::Null).take(array.len()).collect(),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => v.into(),
                None => Value::Null,
            })
            .collect(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap()
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => v.into(),
                None => Value::Null,
            })
            .collect(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap()
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => v.into(),
                None => Value::Null,
            })
            .collect(),
        DataType::Int8 => primitive_array_to_json::<i8>(array),
        DataType::Int16 => primitive_array_to_json::<i16>(array),
        DataType::Int32 => primitive_array_to_json::<i32>(array),
        DataType::Int64 => primitive_array_to_json::<i64>(array),
        DataType::UInt8 => primitive_array_to_json::<u8>(array),
        DataType::UInt16 => primitive_array_to_json::<u16>(array),
        DataType::UInt32 => primitive_array_to_json::<u32>(array),
        DataType::UInt64 => primitive_array_to_json::<u64>(array),
        DataType::Float32 => primitive_array_to_json::<f32>(array),
        DataType::Float64 => primitive_array_to_json::<f64>(array),
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .unwrap()
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => write_array(v.as_ref()),
                None => Value::Null,
            })
            .collect(),
        DataType::LargeList(_) => array
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap()
            .iter()
            .map(|maybe_value| match maybe_value {
                Some(v) => write_array(v.as_ref()),
                None => Value::Null,
            })
            .collect(),
        DataType::Struct(_) => {
            let jsonmaps = struct_array_to_jsonmap_array(
                array.as_any().downcast_ref::<StructArray>().unwrap(),
                array.len(),
            );
            jsonmaps.into_iter().map(Value::Object).collect()
        }
        _ => {
            panic!(format!(
                "Unsupported datatype for array conversion: {:#?}",
                array.data_type()
            ));
        }
    })
}

fn set_column_by_primitive_type<T: NativeType + JsonSerializable>(
    rows: &mut [Map<String, Value>],
    row_count: usize,
    array: &dyn Array,
    col_name: &str,
) {
    let primitive_arr = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    rows.iter_mut()
        .zip(primitive_arr.iter())
        .take(row_count)
        .for_each(|(row, value)| {
            let value = to_json::<T>(value);
            row.insert(col_name.to_string(), value);
        });
}

fn set_column_for_json_rows(
    rows: &mut [Map<String, Value>],
    row_count: usize,
    array: &dyn Array,
    col_name: &str,
) {
    match array.data_type() {
        DataType::Null => {
            // when value is null, we simply skip setting the key
        }
        DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            rows.iter_mut()
                .zip(array.iter())
                .take(row_count)
                .for_each(|(row, value)| {
                    row.insert(
                        col_name.to_string(),
                        value.map(Value::Bool).unwrap_or(Value::Null),
                    );
                });
        }
        DataType::Int8 => set_column_by_primitive_type::<i8>(rows, row_count, array, col_name),
        DataType::Int16 => set_column_by_primitive_type::<i16>(rows, row_count, array, col_name),
        DataType::Int32 => set_column_by_primitive_type::<i32>(rows, row_count, array, col_name),
        DataType::Int64 => set_column_by_primitive_type::<i64>(rows, row_count, array, col_name),
        DataType::UInt8 => set_column_by_primitive_type::<u8>(rows, row_count, array, col_name),
        DataType::UInt16 => set_column_by_primitive_type::<u16>(rows, row_count, array, col_name),
        DataType::UInt32 => set_column_by_primitive_type::<u32>(rows, row_count, array, col_name),
        DataType::UInt64 => set_column_by_primitive_type::<u64>(rows, row_count, array, col_name),
        DataType::Float32 => set_column_by_primitive_type::<f32>(rows, row_count, array, col_name),
        DataType::Float64 => set_column_by_primitive_type::<f64>(rows, row_count, array, col_name),
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            rows.iter_mut()
                .zip(array.iter())
                .take(row_count)
                .for_each(|(row, value)| {
                    row.insert(
                        col_name.to_string(),
                        value
                            .map(|x| Value::String(x.to_string()))
                            .unwrap_or(Value::Null),
                    );
                });
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            rows.iter_mut()
                .zip(array.iter())
                .take(row_count)
                .for_each(|(row, value)| {
                    row.insert(
                        col_name.to_string(),
                        value
                            .map(|x| Value::String(x.to_string()))
                            .unwrap_or(Value::Null),
                    );
                });
        }
        DataType::Struct(_) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let inner_objs = struct_array_to_jsonmap_array(array, row_count);
            rows.iter_mut()
                .take(row_count)
                .zip(inner_objs.into_iter())
                .for_each(|(row, obj)| {
                    row.insert(col_name.to_string(), Value::Object(obj));
                });
        }
        DataType::List(_) => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            rows.iter_mut()
                .zip(array.iter())
                .take(row_count)
                .for_each(|(row, value)| {
                    row.insert(
                        col_name.to_string(),
                        value
                            .map(|x| write_array(x.as_ref()))
                            .unwrap_or(Value::Null),
                    );
                });
        }
        DataType::LargeList(_) => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            rows.iter_mut()
                .zip(array.iter())
                .take(row_count)
                .for_each(|(row, value)| {
                    row.insert(
                        col_name.to_string(),
                        value
                            .map(|x| write_array(x.as_ref()))
                            .unwrap_or(Value::Null),
                    );
                });
        }
        _ => {
            panic!(format!("Unsupported datatype: {:#?}", array.data_type()));
        }
    }
}

/// Serializes a [`RecordBatch`] into Json
/// # Example
/// ```
/// use std::sync::Arc;
/// use arrow2::array::Primitive;
/// use arrow2::datatypes::{DataType, Field, Schema};
/// use arrow2::io::json;
/// use arrow2::record_batch::RecordBatch;
///
/// let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
/// let a = Primitive::from_slice(&[1i32, 2, 3]).to(DataType::Int32);
/// let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
///
/// let json_rows = json::write_record_batches(&[batch]);
/// assert_eq!(
///     serde_json::Value::Object(json_rows[1].clone()),
///     serde_json::json!({"a": 2}),
/// );
/// ```
pub fn write_record_batches(batches: &[RecordBatch]) -> Vec<Map<String, Value>> {
    let mut rows: Vec<Map<String, Value>> = std::iter::repeat(Map::new())
        .take(batches.iter().map(|b| b.num_rows()).sum())
        .collect();

    if !rows.is_empty() {
        let schema = batches[0].schema();
        let mut base = 0;
        batches.iter().for_each(|batch| {
            let row_count = batch.num_rows();
            batch.columns().iter().enumerate().for_each(|(j, col)| {
                let col_name = schema.field(j).name();
                set_column_for_json_rows(&mut rows[base..], row_count, col.as_ref(), col_name);
            });
            base += row_count;
        });
    }
    rows
}
