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

use std::any::Any;

use crate::error::Result;
use crate::{buffer::Bitmap, datatypes::DataType};

pub trait Array: std::fmt::Debug + std::fmt::Display + Send + Sync + ToFFI {
    fn as_any(&self) -> &dyn Any;

    fn len(&self) -> usize;

    fn data_type(&self) -> &DataType;

    fn nulls(&self) -> &Option<Bitmap>;

    #[inline]
    fn null_count(&self) -> usize {
        self.nulls().as_ref().map(|x| x.null_count()).unwrap_or(0)
    }

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.nulls()
            .as_ref()
            .map(|x| !x.get_bit(i))
            .unwrap_or(false)
    }

    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        !self.is_null(i)
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array>;
}

/// Creates a new empty dynamic array
pub fn new_empty_array(data_type: DataType) -> Box<dyn Array> {
    match data_type {
        DataType::Null => Box::new(NullArray::new_empty()),
        DataType::Boolean => Box::new(BooleanArray::new_empty()),
        DataType::Int8 => Box::new(PrimitiveArray::<i8>::new_empty(data_type)),
        DataType::Int16 => Box::new(PrimitiveArray::<i16>::new_empty(data_type)),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            Box::new(PrimitiveArray::<i32>::new_empty(data_type))
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => Box::new(PrimitiveArray::<i64>::new_empty(data_type)),
        DataType::Decimal(_, _) => Box::new(PrimitiveArray::<i128>::new_empty(data_type)),
        DataType::UInt8 => Box::new(PrimitiveArray::<u8>::new_empty(data_type)),
        DataType::UInt16 => Box::new(PrimitiveArray::<u16>::new_empty(data_type)),
        DataType::UInt32 => Box::new(PrimitiveArray::<u32>::new_empty(data_type)),
        DataType::UInt64 => Box::new(PrimitiveArray::<u64>::new_empty(data_type)),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => Box::new(PrimitiveArray::<f32>::new_empty(data_type)),
        DataType::Float64 => Box::new(PrimitiveArray::<f64>::new_empty(data_type)),
        DataType::Binary => Box::new(BinaryArray::<i32>::new_empty()),
        DataType::LargeBinary => Box::new(BinaryArray::<i64>::new_empty()),
        DataType::FixedSizeBinary(_) => Box::new(FixedSizeBinaryArray::new_empty(data_type)),
        DataType::Utf8 => Box::new(Utf8Array::<i32>::new_empty()),
        DataType::LargeUtf8 => Box::new(Utf8Array::<i64>::new_empty()),
        DataType::List(_) => Box::new(ListArray::<i32>::new_empty(data_type)),
        DataType::LargeList(_) => Box::new(ListArray::<i64>::new_empty(data_type)),
        DataType::FixedSizeList(_, _) => Box::new(FixedSizeListArray::new_empty(data_type)),
        DataType::Struct(fields) => Box::new(StructArray::new_empty(&fields)),
        DataType::Union(_) => unimplemented!(),
        DataType::Dictionary(key_type, value_type) => match key_type.as_ref() {
            DataType::Int8 => Box::new(DictionaryArray::<i8>::new_empty(*value_type)),
            DataType::Int16 => Box::new(DictionaryArray::<i16>::new_empty(*value_type)),
            DataType::Int32 => Box::new(DictionaryArray::<i32>::new_empty(*value_type)),
            DataType::Int64 => Box::new(DictionaryArray::<i64>::new_empty(*value_type)),
            DataType::UInt8 => Box::new(DictionaryArray::<u8>::new_empty(*value_type)),
            DataType::UInt16 => Box::new(DictionaryArray::<u16>::new_empty(*value_type)),
            DataType::UInt32 => Box::new(DictionaryArray::<u32>::new_empty(*value_type)),
            DataType::UInt64 => Box::new(DictionaryArray::<u64>::new_empty(*value_type)),
            _ => unreachable!(),
        },
    }
}

macro_rules! clone_dyn {
    ($array:expr, $ty:ty) => {{
        let array = $array.as_any().downcast_ref::<$ty>().unwrap();
        Box::new(array.clone())
    }};
}

/// Clones `array`.
pub fn clone(array: &dyn Array) -> Box<dyn Array> {
    match array.data_type() {
        DataType::Null => clone_dyn!(array, NullArray),
        DataType::Boolean => clone_dyn!(array, BooleanArray),
        DataType::Int8 => clone_dyn!(array, PrimitiveArray<i8>),
        DataType::Int16 => clone_dyn!(array, PrimitiveArray<i16>),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            clone_dyn!(array, PrimitiveArray<i32>)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => clone_dyn!(array, PrimitiveArray<i64>),
        DataType::Decimal(_, _) => clone_dyn!(array, PrimitiveArray<i128>),
        DataType::UInt8 => clone_dyn!(array, PrimitiveArray<u8>),
        DataType::UInt16 => clone_dyn!(array, PrimitiveArray<u16>),
        DataType::UInt32 => clone_dyn!(array, PrimitiveArray<u32>),
        DataType::UInt64 => clone_dyn!(array, PrimitiveArray<u64>),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => clone_dyn!(array, PrimitiveArray<f32>),
        DataType::Float64 => clone_dyn!(array, PrimitiveArray<f64>),
        DataType::Binary => clone_dyn!(array, BinaryArray<i32>),
        DataType::LargeBinary => clone_dyn!(array, BinaryArray<i64>),
        DataType::FixedSizeBinary(_) => clone_dyn!(array, FixedSizeBinaryArray),
        DataType::Utf8 => clone_dyn!(array, Utf8Array::<i32>),
        DataType::LargeUtf8 => clone_dyn!(array, Utf8Array::<i64>),
        DataType::List(_) => clone_dyn!(array, ListArray::<i32>),
        DataType::LargeList(_) => clone_dyn!(array, ListArray::<i64>),
        DataType::FixedSizeList(_, _) => clone_dyn!(array, FixedSizeListArray),
        DataType::Struct(_) => clone_dyn!(array, StructArray),
        DataType::Union(_) => unimplemented!(),
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => clone_dyn!(array, DictionaryArray::<i8>),
            DataType::Int16 => clone_dyn!(array, DictionaryArray::<i16>),
            DataType::Int32 => clone_dyn!(array, DictionaryArray::<i32>),
            DataType::Int64 => clone_dyn!(array, DictionaryArray::<i64>),
            DataType::UInt8 => clone_dyn!(array, DictionaryArray::<u8>),
            DataType::UInt16 => clone_dyn!(array, DictionaryArray::<u16>),
            DataType::UInt32 => clone_dyn!(array, DictionaryArray::<u32>),
            DataType::UInt64 => clone_dyn!(array, DictionaryArray::<u64>),
            _ => unreachable!(),
        }
    }
}

mod binary;
mod boolean;
mod dictionary;
mod fixed_size_binary;
mod fixed_size_list;
mod list;
mod null;
mod primitive;
mod specification;
mod string;
mod struct_;

mod equal;
mod ffi;
pub mod growable;

pub use binary::{BinaryArray, BinaryPrimitive};
pub use boolean::BooleanArray;
pub use dictionary::{DictionaryArray, DictionaryKey, DictionaryPrimitive};
pub use fixed_size_binary::{FixedSizeBinaryArray, FixedSizeBinaryPrimitive};
pub use fixed_size_list::{FixedSizeListArray, FixedSizeListPrimitive};
pub use list::{ListArray, ListPrimitive};
pub use null::NullArray;
pub use primitive::{Primitive, PrimitiveArray};
pub use specification::Offset;
pub use string::{Utf8Array, Utf8Primitive};
pub use struct_::StructArray;

pub use self::ffi::FromFFI;
use self::ffi::ToFFI;

pub type Int8Array = PrimitiveArray<i8>;
pub type Int16Array = PrimitiveArray<i16>;
pub type Int32Array = PrimitiveArray<i32>;
pub type Int64Array = PrimitiveArray<i64>;
pub type Float32Array = PrimitiveArray<f32>;
pub type Float64Array = PrimitiveArray<f64>;
pub type StringArray = Utf8Array<i32>;
pub type UInt8Array = PrimitiveArray<u8>;
pub type UInt16Array = PrimitiveArray<u16>;
pub type UInt32Array = PrimitiveArray<u32>;
pub type UInt64Array = PrimitiveArray<u64>;

pub trait ToArray {
    fn to_arc(self, data_type: &DataType) -> std::sync::Arc<dyn Array>;
}

pub trait TryFromIterator<A>: Sized {
    fn try_from_iter<T: IntoIterator<Item = Result<A>>>(iter: T) -> Result<Self>;
}

pub trait Builder<T>: TryFromIterator<Option<T>> + ToArray {
    fn with_capacity(capacity: usize) -> Self;

    fn push(&mut self, item: Option<&T>);

    #[inline]
    fn try_push(&mut self, item: Option<&T>) -> Result<()> {
        Ok(self.push(item))
    }
}

fn display_helper<T: std::fmt::Display, I: IntoIterator<Item = Option<T>>>(iter: I) -> Vec<String> {
    let iterator = iter.into_iter();
    let len = iterator.size_hint().0;
    if len <= 100 {
        iterator
            .map(|x| match x {
                Some(x) => x.to_string(),
                None => "".to_string(),
            })
            .collect::<Vec<_>>()
    } else {
        iterator
            .enumerate()
            .filter_map(|(i, x)| {
                if i == 5 {
                    Some(format!("...({})...", len - 10))
                } else if i < 5 || i > (len - 5) {
                    Some(match x {
                        Some(x) => x.to_string(),
                        None => "".to_string(),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}

fn display_fmt<T: std::fmt::Display, I: IntoIterator<Item = Option<T>>>(
    iter: I,
    head: &str,
    f: &mut std::fmt::Formatter<'_>,
    new_lines: bool,
) -> std::fmt::Result {
    let result = display_helper(iter);
    if new_lines {
        write!(f, "{}[\n{}\n]", head, result.join(",\n"))
    } else {
        write!(f, "{}[{}]", head, result.join(", "))
    }
}

pub trait IterableBinaryArray: Array {
    unsafe fn value_unchecked(&self, i: usize) -> &[u8];
}

pub trait IterableListArray: Array {
    fn value(&self, i: usize) -> Box<dyn Array>;
}
