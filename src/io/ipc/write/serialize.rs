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
    array::*,
    bitmap::Bitmap,
    datatypes::{DataType, IntervalUnit},
    endianess::is_native_little_endian,
    io::ipc::gen::Message,
    trusted_len::TrustedLen,
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
    write_bitmap(array.validity(), array.len(), buffers, arrow_data, offset);

    write_buffer(
        array.values(),
        buffers,
        arrow_data,
        offset,
        is_little_endian,
    )
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

    write_bitmap(array.validity(), array.len(), buffers, arrow_data, offset);
    write_bitmap(
        &Some(array.values().clone()),
        array.len(),
        buffers,
        arrow_data,
        offset,
    );
}

fn write_generic_binary<O: Offset>(
    validity: &Option<Bitmap>,
    offsets: &[O],
    values: &[u8],
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    write_bitmap(validity, offsets.len() - 1, buffers, arrow_data, offset);

    let first = *offsets.first().unwrap();
    let last = *offsets.last().unwrap();
    if first == O::default() {
        write_buffer(offsets, buffers, arrow_data, offset, is_little_endian);
    } else {
        write_buffer_from_iter(
            offsets.iter().map(|x| *x - first),
            buffers,
            arrow_data,
            offset,
            is_little_endian,
        );
    }

    write_buffer(
        &values[first.to_usize()..last.to_usize()],
        buffers,
        arrow_data,
        offset,
        is_little_endian,
    );
}

fn write_binary<O: Offset>(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let array = array.as_any().downcast_ref::<BinaryArray<O>>().unwrap();
    write_generic_binary(
        array.validity(),
        array.offsets(),
        array.values(),
        buffers,
        arrow_data,
        offset,
        is_little_endian,
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
    write_generic_binary(
        array.validity(),
        array.offsets(),
        array.values(),
        buffers,
        arrow_data,
        offset,
        is_little_endian,
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
    write_bitmap(array.validity(), array.len(), buffers, arrow_data, offset);
    write_buffer(
        array.values(),
        buffers,
        arrow_data,
        offset,
        is_little_endian,
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
    let offsets = array.offsets();
    let validity = array.validity();

    write_bitmap(validity, offsets.len() - 1, buffers, arrow_data, offset);

    let first = *offsets.first().unwrap();
    let last = *offsets.last().unwrap();
    if first == O::default() {
        write_buffer(offsets, buffers, arrow_data, offset, is_little_endian);
    } else {
        write_buffer_from_iter(
            offsets.iter().map(|x| *x - first),
            buffers,
            arrow_data,
            offset,
            is_little_endian,
        );
    }

    write(
        array
            .values()
            .slice(first.to_usize(), last.to_usize() - first.to_usize())
            .as_ref(),
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
    write_bitmap(array.validity(), array.len(), buffers, arrow_data, offset);
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

pub fn write_union(
    array: &dyn Array,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    nodes: &mut Vec<Message::FieldNode>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let array = array.as_any().downcast_ref::<UnionArray>().unwrap();

    write_buffer(array.types(), buffers, arrow_data, offset, is_little_endian);

    if let Some(offsets) = array.offsets() {
        write_buffer(offsets, buffers, arrow_data, offset, is_little_endian);
    }
    array.fields().iter().for_each(|array| {
        write(
            array.as_ref(),
            buffers,
            arrow_data,
            nodes,
            offset,
            is_little_endian,
        )
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
    write_bitmap(array.validity(), array.len(), buffers, arrow_data, offset);
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
        DataType::Union(_, _, _) => {
            write_union(array, buffers, arrow_data, nodes, offset, is_little_endian);
        }
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

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
fn write_bytes_from_iter<I: TrustedLen<Item = u8>>(
    bytes: I,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
) {
    let len = bytes.size_hint().0;
    let pad_len = pad_to_8(len as u32);
    let total_len: i64 = (len + pad_len) as i64;
    // assert_eq!(len % 8, 0, "Buffer width not a multiple of 8 bytes");
    buffers.push(Schema::Buffer::new(*offset, total_len));
    arrow_data.extend(bytes);
    arrow_data.extend_from_slice(&vec![0u8; pad_len][..]);
    *offset += total_len;
}

fn write_bitmap(
    bitmap: &Option<Bitmap>,
    length: usize,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
) {
    match bitmap {
        Some(bitmap) => {
            assert_eq!(bitmap.len(), length);
            let (slice, slice_offset, _) = bitmap.as_slice();
            if slice_offset != 0 {
                // case where we can't slice the bitmap as the offsets are not multiple of 8
                let bytes = Bitmap::from_trusted_len_iter(bitmap.iter());
                let (slice, _, _) = bytes.as_slice();
                write_bytes(slice, buffers, arrow_data, offset)
            } else {
                write_bytes(slice, buffers, arrow_data, offset)
            }
        }
        None => {
            // in IPC, the null bitmap is always be present
            write_bytes_from_iter(
                std::iter::repeat(1).take(length.saturating_add(7) / 8),
                buffers,
                arrow_data,
                offset,
            )
        }
    }
}

#[inline]
fn _write_buffer_from_iter<T: NativeType, I: TrustedLen<Item = T>>(
    buffer: I,
    arrow_data: &mut Vec<u8>,
    is_little_endian: bool,
) {
    let len = buffer.size_hint().0;
    arrow_data.reserve(len * std::mem::size_of::<T>());
    if is_little_endian {
        buffer
            .map(|x| T::to_le_bytes(&x))
            .for_each(|x| arrow_data.extend_from_slice(x.as_ref()))
    } else {
        buffer
            .map(|x| T::to_be_bytes(&x))
            .for_each(|x| arrow_data.extend_from_slice(x.as_ref()))
    }
}

fn _write_buffer<T: NativeType>(buffer: &[T], arrow_data: &mut Vec<u8>, is_little_endian: bool) {
    if is_little_endian == is_native_little_endian() {
        let buffer = unsafe {
            std::slice::from_raw_parts(
                buffer.as_ptr() as *const u8,
                buffer.len() * std::mem::size_of::<T>(),
            )
        };
        arrow_data.extend_from_slice(buffer);
    } else {
        _write_buffer_from_iter(buffer.iter().copied(), arrow_data, is_little_endian)
    }
}

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
fn write_buffer<T: NativeType>(
    buffer: &[T],
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let len = buffer.len() * std::mem::size_of::<T>();
    let pad_len = pad_to_8(len as u32);
    let total_len: i64 = (len + pad_len) as i64;
    // assert_eq!(len % 8, 0, "Buffer width not a multiple of 8 bytes");
    buffers.push(Schema::Buffer::new(*offset, total_len));

    _write_buffer(buffer, arrow_data, is_little_endian);

    arrow_data.extend_from_slice(&vec![0u8; pad_len][..]);
    *offset += total_len;
}

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
fn write_buffer_from_iter<T: NativeType, I: TrustedLen<Item = T>>(
    buffer: I,
    buffers: &mut Vec<Schema::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: &mut i64,
    is_little_endian: bool,
) {
    let len = buffer.size_hint().0 * std::mem::size_of::<T>();
    let pad_len = pad_to_8(len as u32);
    let total_len: i64 = (len + pad_len) as i64;
    // assert_eq!(len % 8, 0, "Buffer width not a multiple of 8 bytes");
    buffers.push(Schema::Buffer::new(*offset, total_len));

    _write_buffer_from_iter(buffer, arrow_data, is_little_endian);

    arrow_data.extend_from_slice(&vec![0u8; pad_len][..]);
    *offset += total_len;
}
