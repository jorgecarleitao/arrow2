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

//! Arrow IPC File and Stream Readers
//!
//! The `FileReader` and `StreamReader` have similar interfaces,
//! however the `FileReader` expects a reader that supports `Seek`ing

use std::{collections::VecDeque, convert::TryInto};
use std::{
    io::{Read, Result, Seek, SeekFrom},
    sync::Arc,
};

use crate::buffer::Buffer;
use crate::datatypes::{DataType, IntervalUnit};
use crate::endianess::is_native_little_endian;
use crate::error::Result as ArrowResult;
use crate::{
    array::*,
    bitmap::Bitmap,
    buffer::MutableBuffer,
    types::{days_ms, NativeType},
};

use super::super::gen;

type Node<'a> = (&'a gen::Message::FieldNode, &'a Option<Arc<dyn Array>>);

fn read_buffer<T: NativeType, R: Read + Seek>(
    buf: &mut VecDeque<&gen::Schema::Buffer>,
    length: usize, // in slots
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<Buffer<T>> {
    let buf = buf.pop_front().unwrap();

    reader.seek(SeekFrom::Start(block_offset + buf.offset() as u64))?;

    let bytes = length * std::mem::size_of::<T>();
    if bytes > buf.length() as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("The slots of the array times the physical size must \
            be smaller or equal to the length of the IPC buffer. \
            However, this array reports {} slots, which, for physical type \"{}\", corresponds to {} bytes, \
            which is larger than the buffer length {}",
                length,
                std::any::type_name::<T>(),
                bytes,
                buf.length(),
            ),
        ));
    }

    // it is undefined behavior to call read_exact on un-initialized, https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read
    // see also https://github.com/MaikKlein/ash/issues/354#issue-781730580
    let mut buffer = MutableBuffer::<T>::from_len_zeroed(length);

    if is_native_little_endian() == is_little_endian {
        // fast case where we can just copy the contents as is
        unsafe {
            // transmute T to bytes.
            let slice = std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, bytes);
            reader.read_exact(slice)?;
        }
    } else {
        // slow case where we must reverse bits
        let mut slice = vec![0u8; bytes];
        reader.read_exact(&mut slice)?;

        if !is_little_endian {
            let chunks = slice.chunks_exact(std::mem::size_of::<T>());
            buffer
                .as_slice_mut()
                .iter_mut()
                .zip(chunks)
                .try_for_each(|(slot, chunk)| {
                    let a: T::Bytes = match chunk.try_into() {
                        Ok(a) => a,
                        Err(_) => unreachable!(),
                    };
                    *slot = T::from_be_bytes(a);
                    ArrowResult::Ok(())
                })
                .unwrap();
        }
    }

    Ok(buffer.into())
}

fn read_bitmap<R: Read + Seek>(
    buf: &mut VecDeque<&gen::Schema::Buffer>,
    length: usize,
    reader: &mut R,
    block_offset: u64,
    _: bool,
) -> Result<Bitmap> {
    let buf = buf.pop_front().unwrap();

    reader.seek(SeekFrom::Start(block_offset + buf.offset() as u64))?;

    let bytes = buf.length() as usize;

    // something is wrong if we can't `length`
    assert!(length <= bytes * 8);
    // it is undefined behavior to call read_exact on un-initialized, https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read
    // see also https://github.com/MaikKlein/ash/issues/354#issue-781730580
    let mut buffer = MutableBuffer::<u8>::from_len_zeroed(bytes);
    reader.read_exact(buffer.as_slice_mut())?;

    Ok(Bitmap::from_bytes(buffer.into(), length))
}

fn read_validity<R: Read + Seek>(
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    field_node: &gen::Message::FieldNode,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<Option<Bitmap>> {
    Ok(if field_node.null_count() > 0 {
        Some(read_bitmap(
            buffers,
            field_node.length() as usize,
            reader,
            block_offset,
            is_little_endian,
        )?)
    } else {
        let _ = buffers.pop_front().unwrap();
        None
    })
}

fn read_primitive<T: NativeType, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<PrimitiveArray<T>>
where
    Vec<u8>: TryInto<T::Bytes>,
{
    let field_node = field_nodes.pop_front().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader, block_offset, is_little_endian)?;

    let values = read_buffer(
        buffers,
        field_node.length() as usize,
        reader,
        block_offset,
        is_little_endian,
    )?;

    let array = PrimitiveArray::<T>::from_data(data_type, values, validity);

    Ok(array)
}

fn read_boolean<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<Arc<dyn Array>> {
    let field_node = field_nodes.pop_front().unwrap().0;

    let length = field_node.length() as usize;
    let validity = read_validity(buffers, &field_node, reader, block_offset, is_little_endian)?;

    let values = read_bitmap(buffers, length, reader, block_offset, is_little_endian)?;

    let array = BooleanArray::from_data(values, validity);
    Ok(Arc::new(array))
}

fn read_utf8<O: Offset, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<Utf8Array<O>>
where
    Vec<u8>: TryInto<O::Bytes> + TryInto<<u8 as NativeType>::Bytes>,
{
    let field_node = field_nodes.pop_front().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader, block_offset, is_little_endian)?;

    let offsets: Buffer<O> = read_buffer(
        buffers,
        1 + field_node.length() as usize,
        reader,
        block_offset,
        is_little_endian,
    )
    // Older versions of the IPC format sometimes do not report an offset
    .or_else(|_| Result::Ok(MutableBuffer::<O>::from(&[O::default()]).into()))?;

    let last_offset = offsets.as_slice()[offsets.len() - 1].to_usize().unwrap();
    let values = read_buffer(buffers, last_offset, reader, block_offset, is_little_endian)?;

    Ok(Utf8Array::<O>::from_data(offsets, values, validity))
}

fn read_binary<O: Offset, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<BinaryArray<O>>
where
    Vec<u8>: TryInto<O::Bytes> + TryInto<<u8 as NativeType>::Bytes>,
{
    let field_node = field_nodes.pop_front().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader, block_offset, is_little_endian)?;

    let offsets: Buffer<O> = read_buffer(
        buffers,
        1 + field_node.length() as usize,
        reader,
        block_offset,
        is_little_endian,
    )
    // Older versions of the IPC format sometimes do not report an offset
    .or_else(|_| Result::Ok(MutableBuffer::<O>::from(&[O::default()]).into()))?;

    let last_offset = offsets.as_slice()[offsets.len() - 1].to_usize().unwrap();
    let values = read_buffer(buffers, last_offset, reader, block_offset, is_little_endian)?;

    Ok(BinaryArray::<O>::from_data(offsets, values, validity))
}

fn read_fixed_size_binary<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<FixedSizeBinaryArray> {
    let field_node = field_nodes.pop_front().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader, block_offset, is_little_endian)?;

    let length =
        field_node.length() as usize * (*FixedSizeBinaryArray::get_size(&data_type) as usize);
    let values = read_buffer(buffers, length, reader, block_offset, is_little_endian)?;

    Ok(FixedSizeBinaryArray::from_data(data_type, values, validity))
}

fn read_null(field_nodes: &mut VecDeque<Node>) -> NullArray {
    NullArray::from_data(field_nodes.pop_front().unwrap().0.length() as usize)
}

fn read_list<O: Offset, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<Arc<dyn Array>>
where
    Vec<u8>: TryInto<O::Bytes>,
{
    let field_node = field_nodes.pop_front().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader, block_offset, is_little_endian)?;

    let offsets = read_buffer::<O, _>(
        buffers,
        1 + field_node.length() as usize,
        reader,
        block_offset,
        is_little_endian,
    )
    // Older versions of the IPC format sometimes do not report an offset
    .or_else(|_| Result::Ok(MutableBuffer::<O>::from(&[O::default()]).into()))?;

    let value_data_type = ListArray::<O>::get_child(&data_type).clone();

    let values = read(
        field_nodes,
        value_data_type,
        buffers,
        reader,
        block_offset,
        is_little_endian,
    )?;
    Ok(Arc::new(ListArray::from_data(
        data_type, offsets, values, validity,
    )))
}

fn read_fixed_size_list<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<Arc<dyn Array>> {
    let field_node = field_nodes.pop_front().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader, block_offset, is_little_endian)?;

    let (value_data_type, _) = FixedSizeListArray::get_child_and_size(&data_type);

    let values = read(
        field_nodes,
        value_data_type.clone(),
        buffers,
        reader,
        block_offset,
        is_little_endian,
    )?;
    Ok(Arc::new(FixedSizeListArray::from_data(
        data_type, values, validity,
    )))
}

fn read_struct<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<Arc<dyn Array>> {
    let field_node = field_nodes.pop_front().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader, block_offset, is_little_endian)?;

    let fields = StructArray::get_fields(&data_type);

    let values = fields
        .iter()
        .map(|field| {
            read(
                field_nodes,
                field.data_type().clone(),
                buffers,
                reader,
                block_offset,
                is_little_endian,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(StructArray::from_data(
        fields.to_vec(),
        values,
        validity,
    )))
}

/// Reads the correct number of buffers based on list type and null_count, and creates a
/// list array ref
pub fn read_dictionary<T: DictionaryKey, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<Arc<dyn Array>>
where
    Vec<u8>: TryInto<T::Bytes>,
{
    let values = field_nodes.front().unwrap().1.as_ref().unwrap();

    let keys = read_primitive(
        field_nodes,
        T::DATA_TYPE,
        buffers,
        reader,
        block_offset,
        is_little_endian,
    )?;

    Ok(Arc::new(DictionaryArray::<T>::from_data(
        keys,
        values.clone(),
    )))
}

pub fn read<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<Arc<dyn Array>> {
    match data_type {
        DataType::Null => {
            let array = read_null(field_nodes);
            Ok(Arc::new(array))
        }
        DataType::Boolean => {
            read_boolean(field_nodes, buffers, reader, block_offset, is_little_endian)
        }
        DataType::Int8 => read_primitive::<i8, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Int16 => read_primitive::<i16, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => read_primitive::<i32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => read_primitive::<i64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Decimal(_, _) => read_primitive::<i128, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Interval(IntervalUnit::DayTime) => read_primitive::<days_ms, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt8 => read_primitive::<u8, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt16 => read_primitive::<u16, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt32 => read_primitive::<u32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt64 => read_primitive::<u64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => read_primitive::<f32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Float64 => read_primitive::<f64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        )
        .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Binary => {
            let array = read_binary::<i32, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            )?;
            Ok(Arc::new(array))
        }
        DataType::LargeBinary => {
            let array = read_binary::<i64, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            )?;
            Ok(Arc::new(array))
        }
        DataType::FixedSizeBinary(_) => {
            let array = read_fixed_size_binary(
                field_nodes,
                data_type,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            )?;
            Ok(Arc::new(array))
        }
        DataType::Utf8 => {
            let array =
                read_utf8::<i32, _>(field_nodes, buffers, reader, block_offset, is_little_endian)?;
            Ok(Arc::new(array))
        }
        DataType::LargeUtf8 => {
            let array =
                read_utf8::<i64, _>(field_nodes, buffers, reader, block_offset, is_little_endian)?;
            Ok(Arc::new(array))
        }
        DataType::List(_) => read_list::<i32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        ),
        DataType::LargeList(_) => read_list::<i64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        ),
        DataType::FixedSizeList(_, _) => read_fixed_size_list(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        ),
        DataType::Struct(_) => read_struct(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
        ),
        DataType::Dictionary(ref key_type, _) => match key_type.as_ref() {
            DataType::Int8 => read_dictionary::<i8, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            ),
            DataType::Int16 => read_dictionary::<i16, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            ),
            DataType::Int32 => read_dictionary::<i32, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            ),
            DataType::Int64 => read_dictionary::<i64, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            ),
            DataType::UInt8 => read_dictionary::<u8, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            ),
            DataType::UInt16 => read_dictionary::<u16, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            ),
            DataType::UInt32 => read_dictionary::<u32, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            ),
            DataType::UInt64 => read_dictionary::<u64, _>(
                field_nodes,
                buffers,
                reader,
                block_offset,
                is_little_endian,
            ),
            _ => unreachable!(),
        },
        DataType::Union(_) => unimplemented!(),
    }
}
