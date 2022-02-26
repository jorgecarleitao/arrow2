use std::borrow::Borrow;
use std::hash::Hasher;
use std::{collections::hash_map::DefaultHasher, sync::Arc};

use hash_hasher::HashedMap;
use indexmap::map::IndexMap as HashMap;
use num_traits::NumCast;
use serde_json::Value;

use crate::{
    array::*,
    bitmap::MutableBitmap,
    chunk::Chunk,
    datatypes::{DataType, Field, IntervalUnit},
    error::ArrowError,
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

fn deserialize_boolean<A: Borrow<Value>>(rows: &[A]) -> BooleanArray {
    let iter = rows.iter().map(|row| match row.borrow() {
        Value::Bool(v) => Some(v),
        _ => None,
    });
    BooleanArray::from_trusted_len_iter(iter)
}

fn deserialize_int<T: NativeType + NumCast, A: Borrow<Value>>(
    rows: &[A],
    data_type: DataType,
) -> PrimitiveArray<T> {
    let iter = rows.iter().map(|row| match row.borrow() {
        Value::Number(number) => number.as_i64().and_then(num_traits::cast::<i64, T>),
        Value::Bool(number) => num_traits::cast::<i32, T>(*number as i32),
        _ => None,
    });
    PrimitiveArray::from_trusted_len_iter(iter).to(data_type)
}

fn deserialize_float<T: NativeType + NumCast, A: Borrow<Value>>(
    rows: &[A],
    data_type: DataType,
) -> PrimitiveArray<T> {
    let iter = rows.iter().map(|row| match row.borrow() {
        Value::Number(number) => number.as_f64().and_then(num_traits::cast::<f64, T>),
        Value::Bool(number) => num_traits::cast::<i32, T>(*number as i32),
        _ => None,
    });
    PrimitiveArray::from_trusted_len_iter(iter).to(data_type)
}

fn deserialize_binary<O: Offset, A: Borrow<Value>>(rows: &[A]) -> BinaryArray<O> {
    let iter = rows.iter().map(|row| match row.borrow() {
        Value::String(v) => Some(v.as_bytes()),
        _ => None,
    });
    BinaryArray::from_trusted_len_iter(iter)
}

fn deserialize_utf8<O: Offset, A: Borrow<Value>>(rows: &[A]) -> Utf8Array<O> {
    let iter = rows.iter().map(|row| match row.borrow() {
        Value::String(v) => Some(v.clone()),
        Value::Number(v) => Some(v.to_string()),
        Value::Bool(v) => Some(v.to_string()),
        _ => None,
    });
    Utf8Array::<O>::from_trusted_len_iter(iter)
}

fn deserialize_list<O: Offset, A: Borrow<Value>>(rows: &[A], data_type: DataType) -> ListArray<O> {
    let child = ListArray::<O>::get_child_type(&data_type);

    let mut validity = MutableBitmap::with_capacity(rows.len());
    let mut offsets = Vec::<O>::with_capacity(rows.len() + 1);
    let mut inner = vec![];
    offsets.push(O::zero());
    rows.iter().fold(O::zero(), |mut length, row| {
        match row.borrow() {
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

    let values = _deserialize(&inner, child.clone());

    ListArray::<O>::from_data(data_type, offsets.into(), values, validity.into())
}

fn deserialize_struct<A: Borrow<Value>>(rows: &[A], data_type: DataType) -> StructArray {
    let fields = StructArray::get_fields(&data_type);

    let mut values = fields
        .iter()
        .map(|f| (&f.name, (f.data_type(), vec![])))
        .collect::<HashMap<_, _>>();

    rows.iter().for_each(|row| {
        match row.borrow() {
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
        .map(|(_, (data_type, values))| _deserialize(&values, data_type.clone()))
        .collect::<Vec<_>>();

    StructArray::from_data(data_type, values, None)
}

fn deserialize_dictionary<K: DictionaryKey, A: Borrow<Value>>(
    rows: &[A],
    data_type: DataType,
) -> DictionaryArray<K> {
    let child = DictionaryArray::<K>::get_child(&data_type);

    let mut map = HashedMap::<u64, K>::default();

    let extractor = build_extract(child);

    let mut inner = vec![];
    let keys = rows
        .iter()
        .map(|x| extractor(x.borrow()))
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
        .collect::<PrimitiveArray<K>>();

    let values = _deserialize(&inner, child.clone());
    DictionaryArray::<K>::from_data(keys, values)
}

pub(crate) fn _deserialize<A: Borrow<Value>>(rows: &[A], data_type: DataType) -> Arc<dyn Array> {
    match &data_type {
        DataType::Null => Arc::new(NullArray::from_data(data_type, rows.len())),
        DataType::Boolean => Arc::new(deserialize_boolean(rows)),
        DataType::Int8 => Arc::new(deserialize_int::<i8, _>(rows, data_type)),
        DataType::Int16 => Arc::new(deserialize_int::<i16, _>(rows, data_type)),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            Arc::new(deserialize_int::<i32, _>(rows, data_type))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            unimplemented!("There is no natural representation of DayTime in JSON.")
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Arc::new(deserialize_int::<i64, _>(rows, data_type)),
        DataType::UInt8 => Arc::new(deserialize_int::<u8, _>(rows, data_type)),
        DataType::UInt16 => Arc::new(deserialize_int::<u16, _>(rows, data_type)),
        DataType::UInt32 => Arc::new(deserialize_int::<u32, _>(rows, data_type)),
        DataType::UInt64 => Arc::new(deserialize_int::<u64, _>(rows, data_type)),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => Arc::new(deserialize_float::<f32, _>(rows, data_type)),
        DataType::Float64 => Arc::new(deserialize_float::<f64, _>(rows, data_type)),
        DataType::Utf8 => Arc::new(deserialize_utf8::<i32, _>(rows)),
        DataType::LargeUtf8 => Arc::new(deserialize_utf8::<i64, _>(rows)),
        DataType::List(_) => Arc::new(deserialize_list::<i32, _>(rows, data_type)),
        DataType::LargeList(_) => Arc::new(deserialize_list::<i64, _>(rows, data_type)),
        DataType::Binary => Arc::new(deserialize_binary::<i32, _>(rows)),
        DataType::LargeBinary => Arc::new(deserialize_binary::<i64, _>(rows)),
        DataType::Struct(_) => Arc::new(deserialize_struct(rows, data_type)),
        DataType::Dictionary(key_type, _, _) => {
            match_integer_type!(key_type, |$T| {
                Arc::new(deserialize_dictionary::<$T, _>(rows, data_type))
            })
        }
        _ => todo!(),
        /*
        DataType::FixedSizeBinary(_) => Box::new(FixedSizeBinaryArray::new_empty(data_type)),
        DataType::FixedSizeList(_, _) => Box::new(FixedSizeListArray::new_empty(data_type)),
        DataType::Decimal(_, _) => Box::new(PrimitiveArray::<i128>::new_empty(data_type)),
        */
    }
}

/// Deserializes `rows` into a [`Chunk`] according to `fields`.
/// This is CPU-bounded.
pub fn deserialize<A: AsRef<str>>(
    rows: &[A],
    fields: &[Field],
) -> Result<Chunk<Arc<dyn Array>>, ArrowError> {
    let data_type = DataType::Struct(fields.to_vec());

    // convert rows to `Value`
    let rows = rows
        .iter()
        .map(|row| {
            let row: Value = serde_json::from_str(row.as_ref()).map_err(ArrowError::from)?;
            Ok(row)
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;

    let (_, columns, _) = deserialize_struct(&rows, data_type).into_data();
    Ok(Chunk::new(columns))
}

/// Deserializes a slice of [`Value`] to an Array of logical type [`DataType`].
///
/// This function allows consuming deserialized JSON to Arrow.
pub fn deserialize_json(rows: &[Value], data_type: DataType) -> Arc<dyn Array> {
    _deserialize(rows, data_type)
}
