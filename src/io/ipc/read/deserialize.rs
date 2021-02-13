//! Arrow IPC File and Stream Readers
//!
//! The `FileReader` and `StreamReader` have similar interfaces,
//! however the `FileReader` expects a reader that supports `Seek`ing

use std::{
    io::{Read, Result, Seek, SeekFrom},
    sync::Arc,
};

use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::{
    array::*,
    buffer::{Bitmap, MutableBuffer, NativeType},
};

use super::super::gen;

type Node<'a> = (&'a gen::Message::FieldNode, &'a Option<Arc<dyn Array>>);

fn read_buffer<T: NativeType, R: Read + Seek>(
    buf: &mut Vec<gen::Schema::Buffer>,
    reader: &mut R,
) -> Result<Buffer<T>> {
    let buf = buf.pop().unwrap();

    reader.seek(SeekFrom::Start(buf.offset() as u64))?;

    // something is wrong if the length is not a multiple
    assert!(buf.length() as usize % std::mem::size_of::<T>() == 0);
    let len = buf.length() as usize / std::mem::size_of::<T>();
    // it is undefined behavior to call read_exact on un-initialized, https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read
    // see also https://github.com/MaikKlein/ash/issues/354#issue-781730580
    let mut buffer = MutableBuffer::<T>::from_len_zeroed(len);
    unsafe {
        // transmute bytes to T.
        let slice =
            std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, buf.length() as usize);
        reader.read_exact(slice)?
    }

    Ok(buffer.into())
}

fn read_bitmap<R: Read + Seek>(
    buf: &mut Vec<gen::Schema::Buffer>,
    length: usize,
    reader: &mut R,
) -> Result<Bitmap> {
    let buf = buf.pop().unwrap();

    reader.seek(SeekFrom::Start(buf.offset() as u64))?;

    let bytes = buf.length() as usize;

    // something is wrong if the length is not a multiple
    assert!(length <= bytes * 8);
    // it is undefined behavior to call read_exact on un-initialized, https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read
    // see also https://github.com/MaikKlein/ash/issues/354#issue-781730580
    let mut buffer = MutableBuffer::<u8>::from_len_zeroed(bytes);
    unsafe {
        // transmute bytes to T.
        let slice =
            std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, buf.length() as usize);
        reader.read_exact(slice)?
    }

    Ok(Bitmap::from_bytes(buffer.into(), length))
}

fn read_validity<R: Read + Seek>(
    buffers: &mut Vec<gen::Schema::Buffer>,
    field_node: &gen::Message::FieldNode,
    reader: &mut R,
) -> Result<Option<Bitmap>> {
    Ok(if field_node.null_count() > 0 {
        Some(read_bitmap(buffers, field_node.length() as usize, reader)?)
    } else {
        let _ = buffers.pop().unwrap();
        None
    })
}

fn read_primitive<T: NativeType, R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<gen::Schema::Buffer>,
    reader: &mut R,
) -> Result<PrimitiveArray<T>> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let values = read_buffer(buffers, reader)?;
    let array = PrimitiveArray::<T>::from_data(data_type, values, validity);

    Ok(array)
}

fn read_boolean<R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    buffers: &mut Vec<gen::Schema::Buffer>,
    reader: &mut R,
) -> Result<Arc<dyn Array>> {
    let field_node = field_nodes.pop().unwrap().0;

    let length = field_node.length() as usize;
    let validity = read_validity(buffers, &field_node, reader)?;

    let values = read_bitmap(buffers, length, reader)?;

    Ok(Arc::new(BooleanArray::from_data(values, validity)))
}

fn read_utf8<O: Offset, R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    buffers: &mut Vec<gen::Schema::Buffer>,
    reader: &mut R,
) -> Result<Utf8Array<O>> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let offsets = read_buffer(buffers, reader)?;
    let values = read_buffer(buffers, reader)?;
    Ok(Utf8Array::<O>::from_data(offsets, values, validity))
}

fn read_binary<O: Offset, R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    buffers: &mut Vec<gen::Schema::Buffer>,
    reader: &mut R,
) -> Result<BinaryArray<O>> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let offsets = read_buffer(buffers, reader)?;
    let values = read_buffer(buffers, reader)?;
    Ok(BinaryArray::<O>::from_data(offsets, values, validity))
}

fn read_fixed_size_binary<R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<gen::Schema::Buffer>,
    reader: &mut R,
) -> Result<FixedSizeBinaryArray> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let values = read_buffer(buffers, reader)?;
    Ok(FixedSizeBinaryArray::from_data(data_type, values, validity))
}

fn read_null(field_nodes: &mut Vec<Node>) -> NullArray {
    NullArray::from_data(field_nodes.pop().unwrap().0.length() as usize)
}

fn read_list<O: Offset, R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<gen::Schema::Buffer>,
    reader: &mut R,
) -> Result<Arc<dyn Array>> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let offsets = read_buffer::<O, _>(buffers, reader)?;

    let value_data_type = ListArray::<O>::get_child(&data_type).clone();

    let values = read(field_nodes, value_data_type, buffers, reader)?.into();
    Ok(Arc::new(ListArray::from_data(
        data_type, offsets, values, validity,
    )))
}

fn read_fixed_size_list<R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<gen::Schema::Buffer>,
    reader: &mut R,
) -> Result<Arc<dyn Array>> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let (value_data_type, _) = FixedSizeListArray::get_child_and_size(&data_type);

    let values = read(field_nodes, value_data_type.clone(), buffers, reader)?.into();
    Ok(Arc::new(FixedSizeListArray::from_data(
        data_type, values, validity,
    )))
}

/// Reads the correct number of buffers based on list type and null_count, and creates a
/// list array ref
pub fn read_dictionary<T: DictionaryKey, R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<gen::Schema::Buffer>,
    reader: &mut R,
) -> Result<Arc<dyn Array>> {
    let values = field_nodes.pop().unwrap().1.as_ref().unwrap();

    let keys = read_primitive(field_nodes, T::DATA_TYPE, buffers, reader)?;

    Ok(Arc::new(DictionaryArray::<T>::from_data(
        keys,
        values.clone(),
    )))
}

pub fn read<R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<gen::Schema::Buffer>,
    reader: &mut R,
) -> Result<Arc<dyn Array>> {
    match data_type {
        DataType::Null => {
            let array = read_null(field_nodes);
            Ok(Arc::new(array))
        }
        DataType::Boolean => read_boolean(field_nodes, buffers, reader),
        DataType::Int8 => read_primitive::<i8, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Int16 => read_primitive::<i16, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            read_primitive::<i32, _>(field_nodes, data_type, buffers, reader)
                .map(|x| Arc::new(x) as Arc<dyn Array>)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => {
            read_primitive::<i64, _>(field_nodes, data_type, buffers, reader)
                .map(|x| Arc::new(x) as Arc<dyn Array>)
        }
        DataType::UInt8 => read_primitive::<u8, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt16 => read_primitive::<u16, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt32 => read_primitive::<u32, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::UInt64 => read_primitive::<u64, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => read_primitive::<f32, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Float64 => read_primitive::<f64, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Arc::new(x) as Arc<dyn Array>),
        DataType::Binary => {
            let array = read_binary::<i32, _>(field_nodes, buffers, reader)?;
            Ok(Arc::new(array))
        }
        DataType::LargeBinary => {
            let array = read_binary::<i64, _>(field_nodes, buffers, reader)?;
            Ok(Arc::new(array))
        }
        DataType::FixedSizeBinary(_) => {
            let array = read_fixed_size_binary(field_nodes, data_type, buffers, reader)?;
            Ok(Arc::new(array))
        }
        DataType::Utf8 => {
            let array = read_utf8::<i32, _>(field_nodes, buffers, reader)?;
            Ok(Arc::new(array))
        }
        DataType::LargeUtf8 => {
            let array = read_utf8::<i64, _>(field_nodes, buffers, reader)?;
            Ok(Arc::new(array))
        }
        DataType::List(_) => read_list::<i32, _>(field_nodes, data_type, buffers, reader),
        DataType::LargeList(_) => read_list::<i64, _>(field_nodes, data_type, buffers, reader),
        DataType::FixedSizeList(_, _) => {
            read_fixed_size_list(field_nodes, data_type, buffers, reader)
        }
        DataType::Struct(_) => unimplemented!(),
        DataType::Union(_) => unimplemented!(),
        DataType::Dictionary(ref key_type, _) => match key_type.as_ref() {
            DataType::Int8 => read_dictionary::<i8, _>(field_nodes, data_type, buffers, reader),
            DataType::Int16 => read_dictionary::<i16, _>(field_nodes, data_type, buffers, reader),
            DataType::Int32 => read_dictionary::<i32, _>(field_nodes, data_type, buffers, reader),
            DataType::Int64 => read_dictionary::<i64, _>(field_nodes, data_type, buffers, reader),
            DataType::UInt8 => read_dictionary::<u8, _>(field_nodes, data_type, buffers, reader),
            DataType::UInt16 => read_dictionary::<u16, _>(field_nodes, data_type, buffers, reader),
            DataType::UInt32 => read_dictionary::<u32, _>(field_nodes, data_type, buffers, reader),
            DataType::UInt64 => read_dictionary::<u64, _>(field_nodes, data_type, buffers, reader),
            _ => unreachable!(),
        },
        DataType::Decimal(_, _) => unimplemented!(),
    }
}
