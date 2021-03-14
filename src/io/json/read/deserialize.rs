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

use std::hash::Hasher;
use std::{collections::hash_map::DefaultHasher, sync::Arc};

use hash_hasher::HashedMap;
use indexmap::map::IndexMap as HashMap;
use num::NumCast;
use serde_json::Value;

use crate::{
    array::{
        Array, BooleanArray, DictionaryArray, DictionaryKey, ListArray, NullArray, Offset,
        Primitive, PrimitiveArray, StructArray, Utf8Array,
    },
    bitmap::MutableBitmap,
    buffer::MutableBuffer,
    datatypes::{DataType, IntervalUnit},
    types::NativeType,
};

/// A function that converts a &Value into an optional tuple of a byte slice and a Value.
/// This is used to create a dictionary, where the hashing depends on the DataType of the child object.
type Extract = Box<dyn Fn(&Value) -> Option<(u64, &Value)>>;

fn build_extract(data_type: &DataType) -> Extract {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => Box::new(|value| match &value {
            Value::String(v) => {
                let mut hasher = DefaultHasher::new();
                hasher.write(v.as_bytes());
                Some((hasher.finish(), value))
            }
            Value::Number(v) => v.as_f64().map(|x| {
                let mut hasher = DefaultHasher::new();
                hasher.write(&x.to_le_bytes());
                (hasher.finish(), value)
            }),
            Value::Bool(v) => {
                let mut hasher = DefaultHasher::new();
                hasher.write(&[*v as u8]);
                Some((hasher.finish(), value))
            }
            _ => None,
        }),
        DataType::Int32 | DataType::Int64 | DataType::Int16 | DataType::Int8 => {
            Box::new(move |value| match &value {
                Value::Number(v) => v.as_f64().map(|x| {
                    let mut hasher = DefaultHasher::new();
                    hasher.write(&x.to_le_bytes());
                    (hasher.finish(), value)
                }),
                Value::Bool(v) => {
                    let mut hasher = DefaultHasher::new();
                    hasher.write(&[*v as u8]);
                    Some((hasher.finish(), value))
                }
                _ => None,
            })
        }
        _ => Box::new(|_| None),
    }
}

fn read_primitive<T: NativeType + NumCast>(
    rows: &[&Value],
    data_type: DataType,
) -> PrimitiveArray<T> {
    let iter = rows.iter().map(|row| match row {
        Value::Number(number) => number.as_f64().and_then(num::cast::cast::<f64, T>),
        Value::Bool(number) => num::cast::cast::<i32, T>(*number as i32),
        _ => None,
    });
    unsafe { Primitive::from_trusted_len_iter(iter) }.to(data_type)
}

fn read_boolean(rows: &[&Value]) -> BooleanArray {
    let iter = rows.iter().map(|row| match row {
        Value::Bool(v) => Some(v),
        _ => None,
    });
    unsafe { BooleanArray::from_trusted_len_iter(iter) }
}

fn read_utf8<O: Offset>(rows: &[&Value]) -> Utf8Array<O> {
    let iter = rows.iter().map(|row| match row {
        Value::String(v) => Some(v.clone()),
        Value::Number(v) => Some(v.to_string()),
        Value::Bool(v) => Some(v.to_string()),
        _ => None,
    });
    unsafe { Utf8Array::<O>::from_trusted_len_iter(iter) }
}

fn read_list<O: Offset>(rows: &[&Value], data_type: DataType) -> ListArray<O> {
    let child = ListArray::<O>::get_child(&data_type);

    let mut validity = MutableBitmap::with_capacity(rows.len());
    let mut inner = Vec::<&Value>::with_capacity(rows.len());
    let mut offsets = MutableBuffer::<O>::with_capacity(rows.len() + 1);
    offsets.push(O::zero());
    rows.iter().fold(O::zero(), |mut length, row| {
        match row {
            Value::Array(value) => {
                inner.extend(value.iter());
                validity.push(true);
                // todo make this an Err
                length += O::from_usize(value.len()).expect("List offset is too large :/");
                offsets.push(length);
                length
            }
            _ => {
                validity.push(false);
                offsets.push(length);
                length
            }
        }
    });

    let values = read(&inner, child.clone());

    ListArray::<O>::from_data(data_type, offsets.into(), values, validity.into())
}

fn read_struct(rows: &[&Value], data_type: DataType) -> StructArray {
    let fields = StructArray::get_fields(&data_type);

    let mut values = fields
        .iter()
        .map(|f| {
            (
                f.name(),
                (f.data_type(), Vec::<&Value>::with_capacity(rows.len())),
            )
        })
        .collect::<HashMap<_, _>>();

    rows.iter().for_each(|row| {
        match row {
            Value::Object(value) => {
                values
                    .iter_mut()
                    .for_each(|(s, (_, inner))| inner.push(value.get(*s).unwrap_or(&Value::Null)));
            }
            _ => {
                values
                    .iter_mut()
                    .for_each(|(_, (_, inner))| inner.push(&Value::Null));
            }
        };
    });

    let values = values
        .into_iter()
        .map(|(_, (data_type, values))| read(&values, data_type.clone()))
        .collect::<Vec<_>>();

    StructArray::from_data(fields.to_vec(), values, None)
}

fn read_dictionary<K: DictionaryKey>(rows: &[&Value], data_type: DataType) -> DictionaryArray<K> {
    let child = DictionaryArray::<K>::get_child(&data_type);

    let mut map = HashedMap::<u64, K>::default();

    let extractor = build_extract(child);

    let mut inner = Vec::<&Value>::with_capacity(rows.len());
    let keys = rows
        .iter()
        .map(|x| extractor(x))
        .map(|item| match item {
            Some((hash, v)) => match map.get(&hash) {
                Some(key) => Some(*key),
                None => {
                    // todo: convert this to an error.
                    let key = K::from_usize(map.len()).unwrap();
                    inner.push(v);
                    map.insert(hash, key);
                    Some(key)
                }
            },
            None => None,
        })
        .collect::<Primitive<K>>()
        .to(K::DATA_TYPE);

    let values = read(&inner, child.clone());
    DictionaryArray::<K>::from_data(keys, values)
}

pub fn read(rows: &[&Value], data_type: DataType) -> Arc<dyn Array> {
    match &data_type {
        DataType::Null => Arc::new(NullArray::from_data(rows.len())),
        DataType::Boolean => Arc::new(read_boolean(rows)),
        DataType::Int8 => Arc::new(read_primitive::<i8>(rows, data_type)),
        DataType::Int16 => Arc::new(read_primitive::<i16>(rows, data_type)),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            Arc::new(read_primitive::<i32>(rows, data_type))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            unimplemented!("There is no natural representation of DayTime in JSON.")
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Arc::new(read_primitive::<i64>(rows, data_type)),
        DataType::UInt8 => Arc::new(read_primitive::<u8>(rows, data_type)),
        DataType::UInt16 => Arc::new(read_primitive::<u16>(rows, data_type)),
        DataType::UInt32 => Arc::new(read_primitive::<u32>(rows, data_type)),
        DataType::UInt64 => Arc::new(read_primitive::<u64>(rows, data_type)),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => Arc::new(read_primitive::<f32>(rows, data_type)),
        DataType::Float64 => Arc::new(read_primitive::<f64>(rows, data_type)),
        DataType::Utf8 => Arc::new(read_utf8::<i32>(rows)),
        DataType::LargeUtf8 => Arc::new(read_utf8::<i64>(rows)),
        DataType::List(_) => Arc::new(read_list::<i32>(rows, data_type)),
        DataType::LargeList(_) => Arc::new(read_list::<i64>(rows, data_type)),
        // unsupported
        //DataType::Binary => Box::new(BinaryArray::<i32>::new_empty()),
        //DataType::LargeBinary => Box::new(BinaryArray::<i64>::new_empty()),
        DataType::Struct(_) => Arc::new(read_struct(rows, data_type)),
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => Arc::new(read_dictionary::<i8>(rows, data_type)),
            DataType::Int16 => Arc::new(read_dictionary::<i16>(rows, data_type)),
            DataType::Int32 => Arc::new(read_dictionary::<i32>(rows, data_type)),
            DataType::Int64 => Arc::new(read_dictionary::<i64>(rows, data_type)),
            DataType::UInt8 => Arc::new(read_dictionary::<u8>(rows, data_type)),
            DataType::UInt16 => Arc::new(read_dictionary::<u16>(rows, data_type)),
            DataType::UInt32 => Arc::new(read_dictionary::<u32>(rows, data_type)),
            DataType::UInt64 => Arc::new(read_dictionary::<u64>(rows, data_type)),
            _ => unreachable!(),
        },
        _ => todo!(),
        /*
        DataType::FixedSizeBinary(_) => Box::new(FixedSizeBinaryArray::new_empty(data_type)),
        DataType::FixedSizeList(_, _) => Box::new(FixedSizeListArray::new_empty(data_type)),
        DataType::Union(_) => unimplemented!(),
        DataType::Decimal(_, _) => Box::new(PrimitiveArray::<i128>::new_empty(data_type)),
        */
    }
}
