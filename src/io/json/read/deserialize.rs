use std::borrow::Borrow;
use std::hash::Hasher;
use std::{collections::hash_map::DefaultHasher, sync::Arc};

use hash_hasher::HashedMap;
use indexmap::map::IndexMap as HashMap;
use json_deserializer::{Number, Value};

use crate::{
    array::*,
    bitmap::MutableBitmap,
    datatypes::{DataType, IntervalUnit},
    error::Error,
    types::NativeType,
};

/// A function that converts a &Value into an optional tuple of a byte slice and a Value.
/// This is used to create a dictionary, where the hashing depends on the DataType of the child object.
type Extract<'a> = Box<dyn Fn(&'a Value<'a>) -> Option<(u64, &'a Value<'a>)>>;

fn build_extract(data_type: &DataType) -> Extract {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => Box::new(|value| match &value {
            Value::String(v) => {
                let mut hasher = DefaultHasher::new();
                hasher.write(v.as_bytes());
                Some((hasher.finish(), value))
            }
            Value::Number(v) => match v {
                Number::Float(_, _) => todo!(),
                Number::Integer(_, _) => todo!(),
            },
            Value::Bool(v) => {
                let mut hasher = DefaultHasher::new();
                hasher.write(&[*v as u8]);
                Some((hasher.finish(), value))
            }
            _ => None,
        }),
        DataType::Int32 | DataType::Int64 | DataType::Int16 | DataType::Int8 => {
            Box::new(move |value| {
                let integer = match value {
                    Value::Number(number) => Some(deserialize_int_single::<i64>(*number)),
                    Value::Bool(number) => Some(if *number { 1i64 } else { 0i64 }),
                    _ => None,
                };
                integer.map(|integer| {
                    let mut hasher = DefaultHasher::new();
                    hasher.write(&integer.to_le_bytes());
                    (hasher.finish(), value)
                })
            })
        }
        _ => Box::new(|_| None),
    }
}

fn deserialize_boolean<'a, A: Borrow<Value<'a>>>(rows: &[A]) -> BooleanArray {
    let iter = rows.iter().map(|row| match row.borrow() {
        Value::Bool(v) => Some(v),
        _ => None,
    });
    BooleanArray::from_trusted_len_iter(iter)
}

fn deserialize_int_single<T>(number: Number) -> T
where
    T: NativeType + lexical_core::FromLexical + Pow10,
{
    match number {
        Number::Float(fraction, exponent) => {
            let integer = fraction.split(|x| *x == b'.').next().unwrap();
            let mut integer: T = lexical_core::parse(integer).unwrap();
            if !exponent.is_empty() {
                let exponent: u32 = lexical_core::parse(exponent).unwrap();
                integer = integer.pow10(exponent);
            }
            integer
        }
        Number::Integer(integer, exponent) => {
            let mut integer: T = lexical_core::parse(integer).unwrap();
            if !exponent.is_empty() {
                let exponent: u32 = lexical_core::parse(exponent).unwrap();
                integer = integer.pow10(exponent);
            }
            integer
        }
    }
}

trait Powi10: NativeType + num_traits::One + std::ops::Add {
    fn powi10(self, exp: i32) -> Self;
}

impl Powi10 for f32 {
    #[inline]
    fn powi10(self, exp: i32) -> Self {
        self * 10.0f32.powi(exp)
    }
}

impl Powi10 for f64 {
    #[inline]
    fn powi10(self, exp: i32) -> Self {
        self * 10.0f64.powi(exp)
    }
}

trait Pow10: NativeType + num_traits::One + std::ops::Add {
    fn pow10(self, exp: u32) -> Self;
}

macro_rules! impl_pow10 {
    ($ty:ty) => {
        impl Pow10 for $ty {
            #[inline]
            fn pow10(self, exp: u32) -> Self {
                self * (10 as $ty).pow(exp)
            }
        }
    };
}
impl_pow10!(u8);
impl_pow10!(u16);
impl_pow10!(u32);
impl_pow10!(u64);
impl_pow10!(i8);
impl_pow10!(i16);
impl_pow10!(i32);
impl_pow10!(i64);

fn deserialize_float_single<T>(number: &Number) -> T
where
    T: NativeType + lexical_core::FromLexical + Powi10,
{
    match number {
        Number::Float(float, exponent) => {
            let mut float: T = lexical_core::parse(float).unwrap();
            if !exponent.is_empty() {
                let exponent: i32 = lexical_core::parse(exponent).unwrap();
                float = float.powi10(exponent);
            }
            float
        }
        Number::Integer(integer, exponent) => {
            let mut float: T = lexical_core::parse(integer).unwrap();
            if !exponent.is_empty() {
                let exponent: i32 = lexical_core::parse(exponent).unwrap();
                float = float.powi10(exponent);
            }
            float
        }
    }
}

fn deserialize_int<'a, T: NativeType + lexical_core::FromLexical + Pow10, A: Borrow<Value<'a>>>(
    rows: &[A],
    data_type: DataType,
) -> PrimitiveArray<T> {
    let iter = rows.iter().map(|row| match row.borrow() {
        Value::Number(number) => Some(deserialize_int_single(*number)),
        Value::Bool(number) => Some(if *number { T::one() } else { T::default() }),
        _ => None,
    });
    PrimitiveArray::from_trusted_len_iter(iter).to(data_type)
}

fn deserialize_float<
    'a,
    T: NativeType + lexical_core::FromLexical + Powi10,
    A: Borrow<Value<'a>>,
>(
    rows: &[A],
    data_type: DataType,
) -> PrimitiveArray<T> {
    let iter = rows.iter().map(|row| match row.borrow() {
        Value::Number(number) => Some(deserialize_float_single(number)),
        Value::Bool(number) => Some(if *number { T::one() } else { T::default() }),
        _ => None,
    });
    PrimitiveArray::from_trusted_len_iter(iter).to(data_type)
}

fn deserialize_binary<'a, O: Offset, A: Borrow<Value<'a>>>(rows: &[A]) -> BinaryArray<O> {
    let iter = rows.iter().map(|row| match row.borrow() {
        Value::String(v) => Some(v.as_bytes()),
        _ => None,
    });
    BinaryArray::from_trusted_len_iter(iter)
}

fn deserialize_utf8<'a, O: Offset, A: Borrow<Value<'a>>>(rows: &[A]) -> Utf8Array<O> {
    let mut array = MutableUtf8Array::<O>::with_capacity(rows.len());
    let mut scratch = vec![];
    for row in rows {
        match row.borrow() {
            Value::String(v) => array.push(Some(v.as_ref())),
            Value::Number(number) => match number {
                Number::Integer(number, exponent) | Number::Float(number, exponent) => {
                    scratch.clear();
                    scratch.extend_from_slice(*number);
                    scratch.push(b'e');
                    scratch.extend_from_slice(*exponent);
                }
            },
            Value::Bool(v) => array.push(Some(if *v { "true" } else { "false" })),
            _ => array.push_null(),
        }
    }
    array.into()
}

fn deserialize_list<'a, O: Offset, A: Borrow<Value<'a>>>(
    rows: &[A],
    data_type: DataType,
) -> ListArray<O> {
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

    ListArray::<O>::new(data_type, offsets.into(), values, validity.into())
}

fn deserialize_struct<'a, A: Borrow<Value<'a>>>(rows: &[A], data_type: DataType) -> StructArray {
    let fields = StructArray::get_fields(&data_type);

    let mut values = fields
        .iter()
        .map(|f| (&f.name, (f.data_type(), vec![])))
        .collect::<HashMap<_, _>>();

    let mut validity = MutableBitmap::with_capacity(rows.len());

    rows.iter().for_each(|row| {
        match row.borrow() {
            Value::Object(value) => {
                values
                    .iter_mut()
                    .for_each(|(s, (_, inner))| inner.push(value.get(*s).unwrap_or(&Value::Null)));
                validity.push(true);
            }
            _ => {
                values
                    .iter_mut()
                    .for_each(|(_, (_, inner))| inner.push(&Value::Null));
                validity.push(false);
            }
        };
    });

    let values = values
        .into_iter()
        .map(|(_, (data_type, values))| _deserialize(&values, data_type.clone()))
        .collect::<Vec<_>>();

    StructArray::new(data_type, values, validity.into())
}

fn deserialize_dictionary<'a, K: DictionaryKey, A: Borrow<Value<'a>>>(
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

pub(crate) fn _deserialize<'a, A: Borrow<Value<'a>>>(
    rows: &[A],
    data_type: DataType,
) -> Arc<dyn Array> {
    match &data_type {
        DataType::Null => Arc::new(NullArray::new(data_type, rows.len())),
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

/// Deserializes a `json` [`Value`] into an [`Array`] of [`DataType`]
/// This is CPU-bounded.
/// # Error
/// This function errors iff either:
/// * `json` is not a [`Value::Array`]
/// * `data_type` is neither [`DataType::List`] nor [`DataType::LargeList`]
pub fn deserialize(json: &Value, data_type: DataType) -> Result<Arc<dyn Array>, Error> {
    match json {
        Value::Array(rows) => match data_type {
            DataType::List(inner) | DataType::LargeList(inner) => {
                Ok(_deserialize(rows, inner.data_type))
            }
            _ => Err(Error::nyi("read an Array from a non-Array data type")),
        },
        _ => Err(Error::nyi("read an Array from a non-Array JSON")),
    }
}
