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

use std::{collections::HashMap, sync::Arc};

use hex;
use num::NumCast;
use serde_json::Value;

use crate::{
    array::*,
    buffer::{days_ms, NativeType},
};
use crate::{
    buffer::{Bitmap, Buffer},
    datatypes::{DataType, Field, IntervalUnit},
};
use crate::{
    datatypes::Schema,
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};

use super::{ArrowJsonBatch, ArrowJsonColumn, ArrowJsonDictionaryBatch};

fn to_validity(validity: &Option<Vec<u8>>) -> Option<Bitmap> {
    validity
        .as_ref()
        .map(|x| x.iter().map(|is_valid| *is_valid == 1).collect::<Bitmap>())
}

fn to_offsets<O: Offset>(offsets: Option<&Vec<Value>>) -> Buffer<O> {
    offsets
        .as_ref()
        .unwrap()
        .iter()
        .map(|v| {
            match v {
                Value::String(s) => s.parse::<i64>().ok(),
                _ => v.as_i64(),
            }
            .map(|x| x as usize)
            .and_then(O::from_usize)
            .unwrap()
        })
        .collect()
}

fn to_interval(value: &Value) -> days_ms {
    if let Value::Object(v) = value {
        let days = v.get("days").unwrap();
        let milliseconds = v.get("milliseconds").unwrap();
        match (days, milliseconds) {
            (Value::Number(days), Value::Number(milliseconds)) => {
                let days = days.as_i64().unwrap() as i32;
                let milliseconds = milliseconds.as_i64().unwrap() as i32;
                days_ms::new(days, milliseconds)
            }
            (_, _) => panic!(),
        }
    } else {
        panic!()
    }
}

fn to_primitive_interval(
    json_col: &ArrowJsonColumn,
    data_type: DataType,
) -> PrimitiveArray<days_ms> {
    let validity = to_validity(&json_col.validity);
    let values = json_col
        .data
        .as_ref()
        .unwrap()
        .iter()
        .map(to_interval)
        .collect();
    PrimitiveArray::<days_ms>::from_data(data_type, values, validity)
}

fn to_decimal(json_col: &ArrowJsonColumn, data_type: DataType) -> PrimitiveArray<i128> {
    let validity = to_validity(&json_col.validity);
    let values = json_col
        .data
        .as_ref()
        .unwrap()
        .iter()
        .map(|value| match value {
            Value::String(x) => x.parse::<i128>().unwrap(),
            _ => {
                panic!()
            }
        })
        .collect();

    PrimitiveArray::<i128>::from_data(data_type, values, validity)
}

fn to_primitive<T: NativeType + NumCast>(
    json_col: &ArrowJsonColumn,
    data_type: DataType,
) -> PrimitiveArray<T> {
    let validity = to_validity(&json_col.validity);
    let values = if data_type == DataType::Float64 || data_type == DataType::Float32 {
        json_col
            .data
            .as_ref()
            .unwrap()
            .iter()
            .map(|value| value.as_f64().and_then(num::cast::cast::<f64, T>).unwrap())
            .collect()
    } else {
        json_col
            .data
            .as_ref()
            .unwrap()
            .iter()
            .map(|value| match value {
                Value::Number(x) => x.as_i64().and_then(num::cast::cast::<i64, T>).unwrap(),
                Value::String(x) => {
                    println!("{:?}", x);
                    x.parse::<i64>()
                        .ok()
                        .and_then(num::cast::cast::<i64, T>)
                        .unwrap()
                }
                _ => {
                    panic!()
                }
            })
            .collect()
    };

    PrimitiveArray::<T>::from_data(data_type, values, validity)
}

fn to_binary<O: Offset>(json_col: &ArrowJsonColumn) -> Arc<dyn Array> {
    let validity = to_validity(&json_col.validity);
    let offsets = to_offsets::<O>(json_col.offset.as_ref());
    let values = json_col
        .data
        .as_ref()
        .unwrap()
        .iter()
        .map(|value| value.as_str().map(|x| hex::decode(x).unwrap()).unwrap())
        .flatten()
        .collect();
    Arc::new(BinaryArray::from_data(offsets, values, validity))
}

fn to_utf8<O: Offset>(json_col: &ArrowJsonColumn) -> Arc<dyn Array> {
    let validity = to_validity(&json_col.validity);
    let offsets = to_offsets::<O>(json_col.offset.as_ref());
    let values = json_col
        .data
        .as_ref()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().as_bytes().to_vec())
        .flatten()
        .collect();
    Arc::new(Utf8Array::from_data(offsets, values, validity))
}

fn to_list<O: Offset>(
    json_col: &ArrowJsonColumn,
    data_type: DataType,
    dictionaries: &HashMap<i64, &ArrowJsonDictionaryBatch>,
) -> Result<Arc<dyn Array>> {
    let validity = json_col
        .validity
        .as_ref()
        .map(|x| x.iter().map(|is_valid| *is_valid == 1).collect::<Bitmap>());

    let child_field = ListArray::<O>::get_child_field(&data_type);
    let children = &json_col.children.as_ref().unwrap()[0];
    let values = to_array(&child_field, children, dictionaries)?;
    let offsets = to_offsets::<O>(json_col.offset.as_ref());
    Ok(Arc::new(ListArray::<O>::from_data(
        data_type, offsets, values, validity,
    )))
}

fn to_dictionary<K: DictionaryKey>(
    field: &Field,
    json_col: &ArrowJsonColumn,
    dictionaries: &HashMap<i64, &ArrowJsonDictionaryBatch>,
) -> Result<Arc<dyn Array>> {
    let dict_id = field
        .dict_id()
        .ok_or_else(|| ArrowError::IPC(format!("Unable to find dict_id for field {:?}", field)))?;
    // find dictionary
    let dictionary = dictionaries
        .get(&dict_id)
        .ok_or_else(|| ArrowError::IPC(format!("Unable to find any dictionary id {}", dict_id)))?;

    let keys = to_primitive(json_col, K::DATA_TYPE);

    // todo: make DataType::Dictionary hold a Field so that it can hold dictionary_id
    let data_type = DictionaryArray::<K>::get_child(field.data_type());
    // note: not enough info on nullability of dictionary
    let value_field = Field::new("value", data_type.clone(), true);
    let values = to_array(&value_field, &dictionary.data.columns[0], &HashMap::new())?;

    Ok(Arc::new(DictionaryArray::<K>::from_data(keys, values)))
}

/// Construct an [`Array`] from the JSON integration format
pub fn to_array(
    field: &Field,
    json_col: &ArrowJsonColumn,
    dictionaries: &HashMap<i64, &ArrowJsonDictionaryBatch>,
) -> Result<Arc<dyn Array>> {
    let data_type = field.data_type();
    match data_type {
        DataType::Null => Ok(Arc::new(NullArray::from_data(json_col.count))),
        DataType::Boolean => {
            let array = json_col
                .validity
                .as_ref()
                .unwrap()
                .iter()
                .zip(json_col.data.as_ref().unwrap())
                .map(|(is_valid, value)| {
                    if *is_valid == 1 {
                        Some(value.as_bool().unwrap())
                    } else {
                        None
                    }
                })
                .collect::<BooleanArray>();
            Ok(Arc::new(array))
        }
        DataType::Int8 => Ok(Arc::new(to_primitive::<i8>(json_col, data_type.clone()))),
        DataType::Int16 => Ok(Arc::new(to_primitive::<i16>(json_col, data_type.clone()))),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            Ok(Arc::new(to_primitive::<i32>(json_col, data_type.clone())))
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Ok(Arc::new(to_primitive::<i64>(json_col, data_type.clone()))),
        DataType::Interval(IntervalUnit::DayTime) => {
            Ok(Arc::new(to_primitive_interval(json_col, data_type.clone())))
        }
        DataType::Decimal(_, _) => Ok(Arc::new(to_decimal(json_col, data_type.clone()))),
        DataType::UInt8 => Ok(Arc::new(to_primitive::<u8>(json_col, data_type.clone()))),
        DataType::UInt16 => Ok(Arc::new(to_primitive::<u16>(json_col, data_type.clone()))),
        DataType::UInt32 => Ok(Arc::new(to_primitive::<u32>(json_col, data_type.clone()))),
        DataType::UInt64 => Ok(Arc::new(to_primitive::<u64>(json_col, data_type.clone()))),
        DataType::Float32 => Ok(Arc::new(to_primitive::<f32>(json_col, data_type.clone()))),
        DataType::Float64 => Ok(Arc::new(to_primitive::<f64>(json_col, data_type.clone()))),
        DataType::Binary => Ok(to_binary::<i32>(json_col)),
        DataType::LargeBinary => Ok(to_binary::<i64>(json_col)),
        DataType::Utf8 => Ok(to_utf8::<i32>(json_col)),
        DataType::LargeUtf8 => Ok(to_utf8::<i64>(json_col)),
        DataType::FixedSizeBinary(_) => {
            let validity = to_validity(&json_col.validity);

            let values = json_col
                .data
                .as_ref()
                .unwrap()
                .iter()
                .map(|value| value.as_str().map(|x| hex::decode(x).unwrap()).unwrap())
                .flatten()
                .collect();
            Ok(Arc::new(FixedSizeBinaryArray::from_data(
                data_type.clone(),
                values,
                validity,
            )))
        }

        DataType::List(_) => to_list::<i32>(json_col, data_type.clone(), dictionaries),
        DataType::LargeList(_) => to_list::<i64>(json_col, data_type.clone(), dictionaries),

        DataType::FixedSizeList(child_field, _) => {
            let validity = to_validity(&json_col.validity);

            let children = &json_col.children.as_ref().unwrap()[0];
            let values = to_array(&child_field, children, dictionaries)?;

            Ok(Arc::new(FixedSizeListArray::from_data(
                data_type.clone(),
                values,
                validity,
            )))
        }
        DataType::Struct(fields) => {
            let validity = to_validity(&json_col.validity);

            let values = fields
                .iter()
                .zip(json_col.children.as_ref().unwrap())
                .map(|(field, col)| to_array(field, col, dictionaries))
                .collect::<Result<Vec<_>>>()?;

            let array = StructArray::from_data(fields.clone(), values, validity);
            Ok(Arc::new(array))
        }
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => to_dictionary::<i8>(field, json_col, dictionaries),
            DataType::Int16 => to_dictionary::<i16>(field, json_col, dictionaries),
            DataType::Int32 => to_dictionary::<i32>(field, json_col, dictionaries),
            DataType::Int64 => to_dictionary::<i64>(field, json_col, dictionaries),
            DataType::UInt8 => to_dictionary::<u8>(field, json_col, dictionaries),
            DataType::UInt16 => to_dictionary::<u16>(field, json_col, dictionaries),
            DataType::UInt32 => to_dictionary::<u32>(field, json_col, dictionaries),
            DataType::UInt64 => to_dictionary::<u64>(field, json_col, dictionaries),
            _ => unreachable!(),
        },
        DataType::Float16 => unreachable!(),
        DataType::Union(_) => Err(ArrowError::NotYetImplemented(
            "Union not supported".to_string(),
        )),
    }
}

pub fn to_record_batch(
    schema: &Schema,
    json_batch: &ArrowJsonBatch,
    json_dictionaries: &HashMap<i64, &ArrowJsonDictionaryBatch>,
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .zip(&json_batch.columns)
        .map(|(field, json_col)| to_array(field, &json_col, json_dictionaries))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema.clone(), columns)
}
