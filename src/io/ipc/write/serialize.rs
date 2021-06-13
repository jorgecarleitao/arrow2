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

use crate::{
    array::{
        Array, BinaryArray, BooleanArray, DictionaryArray, DictionaryKey, FixedSizeBinaryArray,
        FixedSizeListArray, ListArray, Offset, PrimitiveArray, StructArray, Utf8Array,
    },
    bitmap::Bitmap,
    datatypes::{DataType, IntervalUnit},
    io::ipc::gen::Message,
    types::{days_ms, NativeType},
};

use crate::io::ipc::gen::Schema;

use super::common::pad_to_8;

fn _write_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    write_bytes(
        &to_le_bytes_bitmap(array.validity(), array.len()),
        buffers,
        arrow_data,
        offset,
    );
    write_bytes(
        &to_bytes(array.values(), is_little_endian),
        buffers,
        arrow_data,
        offset,
    );
}

fn write_primitive<T: NativeType>(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    _write_primitive(array, buffers, arrow_data, offset, is_little_endian);
}

fn write_boolean(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
    _: bool,
) {
    let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    write_bytes(
        &to_le_bytes_bitmap(array.validity(), array.len()),
        buffers,
        arrow_data,
        offset,
    );
    write_bytes(&to_le_bitmap(array.values()), buffers, arrow_data, offset);
}

fn write_binary<O: Offset>(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let array = array.as_any().downcast_ref::<BinaryArray<O>>().unwrap();
    write_bytes(
        &to_le_bytes_bitmap(array.validity(), array.len()),
        buffers,
        arrow_data,
        offset,
    );
    write_bytes(
        &to_bytes(array.offsets(), is_little_endian),
        buffers,
        arrow_data,
        offset,
    );
    write_bytes(
        &to_bytes(array.values(), is_little_endian),
        buffers,
        arrow_data,
        offset,
    );
}

fn write_utf8<O: Offset>(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let array = array.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
    write_bytes(
        &to_le_bytes_bitmap(array.validity(), array.len()),
        buffers,
        arrow_data,
        offset,
    );
    write_bytes(
        &to_bytes(array.offsets(), is_little_endian),
        buffers,
        arrow_data,
        offset,
    );
    write_bytes(
        &to_bytes(array.values(), is_little_endian),
        buffers,
        arrow_data,
        offset,
    );
}

fn write_fixed_size_binary(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let array = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    write_bytes(
        &to_le_bytes_bitmap(array.validity(), array.len()),
        buffers,
        arrow_data,
        offset,
    );
    write_bytes(
        &to_bytes(array.values(), is_little_endian),
        buffers,
        arrow_data,
        offset,
    );
}

fn write_list<O: Offset>(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    nodes: &mut Vec<Message::FieldNode>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let array = array.as_any().downcast_ref::<ListArray<O>>().unwrap();
    write_bytes(
        &to_le_bytes_bitmap(array.validity(), array.len()),
        buffers,
        arrow_data,
        offset,
    );
    write_bytes(
        &to_bytes(array.offsets(), is_little_endian),
        buffers,
        arrow_data,
        offset,
    );
    write(
        array.values().as_ref(),
        buffers,
        arrow_data,
        nodes,
        offset,
        is_little_endian,
    );
}

pub fn write_struct(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    nodes: &mut Vec<Message::FieldNode>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let array = array.as_any().downcast_ref::<StructArray>().unwrap();
    write_bytes(
        &to_le_bytes_bitmap(array.validity(), array.len()),
        buffers,
        arrow_data,
        offset,
    );
    array.values().iter().for_each(|array| {
        write(
            array.as_ref(),
            buffers,
            arrow_data,
            nodes,
            offset,
            is_little_endian,
        );
    });
}

fn write_fixed_size_list(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    nodes: &mut Vec<Message::FieldNode>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    write_bytes(
        &to_le_bytes_bitmap(array.validity(), array.len()),
        buffers,
        arrow_data,
        offset,
    );
    write(
        array.values().as_ref(),
        buffers,
        arrow_data,
        nodes,
        offset,
        is_little_endian,
    );
}

// use `write_keys` to either write keys or values
pub fn _write_dictionary<K: DictionaryKey>(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    nodes: &mut Vec<Message::FieldNode>,
    offset: &mut i64,
    is_little_endian: bool,
    write_keys: bool,
) -> usize {
    let array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
    if write_keys {
        _write_primitive(array.keys(), buffers, arrow_data, offset, is_little_endian);
        array.keys().len()
    } else {
        write(
            array.values().as_ref(),
            buffers,
            arrow_data,
            nodes,
            offset,
            is_little_endian,
        );
        array.values().len()
    }
}

pub fn write_dictionary(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    nodes: &mut Vec<Message::FieldNode>,
    offset: &mut i64,
    is_little_endian: bool,
    write_keys: bool,
) -> usize {
    match array.data_type() {
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => _write_dictionary::<i8>(
                array,
                buffers,
                arrow_data,
                nodes,
                offset,
                is_little_endian,
                write_keys,
            ),
            DataType::Int16 => _write_dictionary::<i16>(
                array,
                buffers,
                arrow_data,
                nodes,
                offset,
                is_little_endian,
                write_keys,
            ),
            DataType::Int32 => _write_dictionary::<i32>(
                array,
                buffers,
                arrow_data,
                nodes,
                offset,
                is_little_endian,
                write_keys,
            ),
            DataType::Int64 => _write_dictionary::<i64>(
                array,
                buffers,
                arrow_data,
                nodes,
                offset,
                is_little_endian,
                write_keys,
            ),
            DataType::UInt8 => _write_dictionary::<u8>(
                array,
                buffers,
                arrow_data,
                nodes,
                offset,
                is_little_endian,
                write_keys,
            ),
            DataType::UInt16 => _write_dictionary::<u16>(
                array,
                buffers,
                arrow_data,
                nodes,
                offset,
                is_little_endian,
                write_keys,
            ),
            DataType::UInt32 => _write_dictionary::<u32>(
                array,
                buffers,
                arrow_data,
                nodes,
                offset,
                is_little_endian,
                write_keys,
            ),
            DataType::UInt64 => _write_dictionary::<u64>(
                array,
                buffers,
                arrow_data,
                nodes,
                offset,
                is_little_endian,
                write_keys,
            ),
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

pub fn write(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    nodes: &mut Vec<Message::FieldNode>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    nodes.push(Message::FieldNode::new(
        array.len() as i64,
        array.null_count() as i64,
    ));
    match array.data_type() {
        DataType::Null => (),
        DataType::Boolean => write_boolean(array, buffers, arrow_data, offset, is_little_endian),
        DataType::Int8 => {
            write_primitive::<i8>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::Int16 => {
            write_primitive::<i16>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            write_primitive::<i32>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            write_primitive::<i64>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::Decimal(_, _) => {
            write_primitive::<i128>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            write_primitive::<days_ms>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::UInt8 => {
            write_primitive::<u8>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::UInt16 => {
            write_primitive::<u16>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::UInt32 => {
            write_primitive::<u32>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::UInt64 => {
            write_primitive::<u64>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::Float16 => unreachable!(),
        DataType::Float32 => {
            write_primitive::<f32>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::Float64 => {
            write_primitive::<f64>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::Binary => {
            write_binary::<i32>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::LargeBinary => {
            write_binary::<i64>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::FixedSizeBinary(_) => {
            write_fixed_size_binary(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::Utf8 => write_utf8::<i32>(array, buffers, arrow_data, offset, is_little_endian),
        DataType::LargeUtf8 => {
            write_utf8::<i64>(array, buffers, arrow_data, offset, is_little_endian)
        }
        DataType::List(_) => {
            write_list::<i32>(array, buffers, arrow_data, nodes, offset, is_little_endian)
        }
        DataType::LargeList(_) => {
            write_list::<i64>(array, buffers, arrow_data, nodes, offset, is_little_endian)
        }
        DataType::FixedSizeList(_, _) => {
            write_fixed_size_list(array, buffers, arrow_data, nodes, offset, is_little_endian)
        }
        DataType::Struct(_) => {
            write_struct(array, buffers, arrow_data, nodes, offset, is_little_endian)
        }
        DataType::Dictionary(_, _) => {
            write_dictionary(
                array,
                buffers,
                arrow_data,
                nodes,
                offset,
                is_little_endian,
                true,
            );
        }
        DataType::Union(_) => unimplemented!(),
    }
}

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
#[inline]
fn write_bytes(
    bytes: &[u8],
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
) {
    let len = bytes.len();
    let pad_len = pad_to_8(len as u32);
    let total_len: i64 = (len + pad_len) as i64;
    // assert_eq!(len % 8, 0, "Buffer width not a multiple of 8 bytes");
    buffers.push(Schema::Buffer::new(*offset, total_len));
    arrow_data.extend_from_slice(bytes);
    arrow_data.extend_from_slice(&vec![0u8; pad_len][..]);
    *offset += total_len;
}

/// converts the buffer to a bytes in little endian
#[inline]
fn to_bytes<T: NativeType>(values: &[T], is_little_endian: bool) -> Vec<u8> {
    if is_little_endian {
        values
            .iter()
            .map(T::to_le_bytes)
            .map(|x| x.as_ref().to_vec())
            .flatten()
            .collect::<Vec<_>>()
    } else {
        values
            .iter()
            .map(T::to_be_bytes)
            .map(|x| x.as_ref().to_vec())
            .flatten()
            .collect::<Vec<_>>()
    }
}

#[inline]
fn to_le_bitmap(bitmap: &Bitmap) -> Vec<u8> {
    if bitmap.offset() != 0 {
        // case where we can't slice the bitmap as the offsets are not multiple of 8
        Bitmap::from_trusted_len_iter(bitmap.iter())
            .as_slice()
            .to_vec()
    } else {
        bitmap.as_slice().to_vec()
    }
}

#[inline]
fn to_le_bytes_bitmap(bitmap: &Option<Bitmap>, length: usize) -> Vec<u8> {
    match bitmap {
        Some(bitmap) => {
            assert_eq!(bitmap.len(), length);
            to_le_bitmap(bitmap)
        }
        None => {
            // in IPC, the null bitmap is always be present
            let bitmap = Bitmap::from_trusted_len_iter(std::iter::repeat(true).take(length));
            bitmap.as_slice().to_vec()
        }
    }
}
