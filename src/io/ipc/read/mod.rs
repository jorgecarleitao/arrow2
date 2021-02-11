//! Arrow IPC File and Stream Readers
//!
//! The `FileReader` and `StreamReader` have similar interfaces,
//! however the `FileReader` expects a reader that supports `Seek`ing

use std::io::{Read, Result, Seek, SeekFrom};

use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::io::ipc;
use crate::{
    array::*,
    buffer::{Bitmap, MutableBuffer, NativeType},
};

type Node = (ipc::FieldNode, Box<dyn Array>);

fn read_buffer<T: NativeType, R: Read + Seek>(
    buf: &mut Vec<ipc::Buffer>,
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
    buf: &mut Vec<ipc::Buffer>,
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
    buffers: &mut Vec<ipc::Buffer>,
    field_node: &ipc::FieldNode,
    reader: &mut R,
) -> Result<Option<Bitmap>> {
    Ok(if field_node.null_count() > 0 {
        Some(read_bitmap(buffers, field_node.length() as usize, reader)?)
    } else {
        let _ = buffers.pop().unwrap();
        None
    })
}

fn read_primitive_array<T: NativeType, R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<ipc::Buffer>,
    reader: &mut R,
) -> Result<PrimitiveArray<T>> {
    let field_node = field_nodes.pop().unwrap().0;
    let length = field_node.length() as usize;
    let null_count = field_node.null_count() as usize;

    let validity = read_validity(buffers, &field_node, reader)?;

    let values = read_buffer(buffers, reader)?;
    let array = PrimitiveArray::<T>::from_data(data_type, values, validity);

    Ok(array)
}

fn read_boolean_array<R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    buffers: &mut Vec<ipc::Buffer>,
    reader: &mut R,
) -> Result<Box<dyn Array>> {
    let field_node = field_nodes.pop().unwrap().0;

    let length = field_node.length() as usize;
    let validity = read_validity(buffers, &field_node, reader)?;

    let values = read_bitmap(buffers, length, reader)?;

    Ok(Box::new(BooleanArray::from_data(values, validity)))
}

fn read_utf8_array<O: Offset, R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    buffers: &mut Vec<ipc::Buffer>,
    reader: &mut R,
) -> Result<Utf8Array<O>> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let offsets = read_buffer(buffers, reader)?;
    let values = read_buffer(buffers, reader)?;
    Ok(Utf8Array::<O>::from_data(offsets, values, validity))
}

fn read_binary_array<O: Offset, R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    buffers: &mut Vec<ipc::Buffer>,
    reader: &mut R,
) -> Result<BinaryArray<O>> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let offsets = read_buffer(buffers, reader)?;
    let values = read_buffer(buffers, reader)?;
    Ok(BinaryArray::<O>::from_data(offsets, values, validity))
}

fn read_fixed_size_binary_array<R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<ipc::Buffer>,
    reader: &mut R,
) -> Result<FixedSizeBinaryArray> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let values = read_buffer(buffers, reader)?;
    Ok(FixedSizeBinaryArray::from_data(data_type, values, validity))
}

fn read_null_array(field_nodes: &mut Vec<Node>) -> NullArray {
    NullArray::from_data(field_nodes.pop().unwrap().0.length() as usize)
}

fn read_list_array<O: Offset, R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<ipc::Buffer>,
    reader: &mut R,
) -> Result<Box<dyn Array>> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let offsets = read_buffer::<O, _>(buffers, reader)?;

    let value_data_type = ListArray::<O>::get_child(&data_type).clone();

    let values = read_array(field_nodes, value_data_type, buffers, reader)?.into();
    Ok(Box::new(ListArray::from_data(
        data_type, offsets, values, validity,
    )))
}

fn read_fixed_size_list_array<R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<ipc::Buffer>,
    reader: &mut R,
) -> Result<Box<dyn Array>> {
    let field_node = field_nodes.pop().unwrap().0;

    let validity = read_validity(buffers, &field_node, reader)?;

    let (value_data_type, _) = FixedSizeListArray::get_child_and_size(&data_type);

    let values = read_array(field_nodes, value_data_type.clone(), buffers, reader)?.into();
    Ok(Box::new(FixedSizeListArray::from_data(
        data_type, values, validity,
    )))
}

/// Reads the correct number of buffers based on list type and null_count, and creates a
/// list array ref
fn read_dictionary_array<T: DictionaryKey, R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<ipc::Buffer>,
    reader: &mut R,
) -> Result<Box<dyn Array>> {
    let values: std::sync::Arc<dyn Array> = field_nodes.pop().unwrap().1.into();

    let (child, values_data_type) = DictionaryArray::<T>::get_child(&data_type);

    assert_eq!(values_data_type, values.data_type());

    let keys = read_primitive_array(field_nodes, child.clone(), buffers, reader)?;

    Ok(Box::new(DictionaryArray::<T>::from_data(keys, values)))
}

pub fn read_array<R: Read + Seek>(
    field_nodes: &mut Vec<Node>,
    data_type: DataType,
    buffers: &mut Vec<ipc::Buffer>,
    reader: &mut R,
) -> Result<Box<dyn Array>> {
    match data_type {
        DataType::Null => {
            let array = read_null_array(field_nodes);
            Ok(Box::new(array))
        }
        DataType::Boolean => read_boolean_array(field_nodes, buffers, reader),
        DataType::Int8 => read_primitive_array::<i8, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Box::new(x) as Box<dyn Array>),
        DataType::Int16 => read_primitive_array::<i16, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Box::new(x) as Box<dyn Array>),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            read_primitive_array::<i32, _>(field_nodes, data_type, buffers, reader)
                .map(|x| Box::new(x) as Box<dyn Array>)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => {
            read_primitive_array::<i64, _>(field_nodes, data_type, buffers, reader)
                .map(|x| Box::new(x) as Box<dyn Array>)
        }
        DataType::UInt8 => read_primitive_array::<u8, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Box::new(x) as Box<dyn Array>),
        DataType::UInt16 => read_primitive_array::<u16, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Box::new(x) as Box<dyn Array>),
        DataType::UInt32 => read_primitive_array::<u32, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Box::new(x) as Box<dyn Array>),
        DataType::UInt64 => read_primitive_array::<u64, _>(field_nodes, data_type, buffers, reader)
            .map(|x| Box::new(x) as Box<dyn Array>),
        DataType::Float16 => unreachable!(),
        DataType::Float32 => {
            read_primitive_array::<f32, _>(field_nodes, data_type, buffers, reader)
                .map(|x| Box::new(x) as Box<dyn Array>)
        }
        DataType::Float64 => {
            read_primitive_array::<f64, _>(field_nodes, data_type, buffers, reader)
                .map(|x| Box::new(x) as Box<dyn Array>)
        }
        DataType::Binary => {
            let array = read_binary_array::<i32, _>(field_nodes, buffers, reader)?;
            Ok(Box::new(array))
        }
        DataType::LargeBinary => {
            let array = read_binary_array::<i64, _>(field_nodes, buffers, reader)?;
            Ok(Box::new(array))
        }
        DataType::FixedSizeBinary(_) => {
            let array = read_fixed_size_binary_array(field_nodes, data_type, buffers, reader)?;
            Ok(Box::new(array))
        }
        DataType::Utf8 => {
            let array = read_utf8_array::<i32, _>(field_nodes, buffers, reader)?;
            Ok(Box::new(array))
        }
        DataType::LargeUtf8 => {
            let array = read_utf8_array::<i64, _>(field_nodes, buffers, reader)?;
            Ok(Box::new(array))
        }
        DataType::List(_) => read_list_array::<i32, _>(field_nodes, data_type, buffers, reader),
        DataType::LargeList(_) => {
            read_list_array::<i64, _>(field_nodes, data_type, buffers, reader)
        }
        DataType::FixedSizeList(_, _) => {
            read_fixed_size_list_array(field_nodes, data_type, buffers, reader)
        }
        DataType::Struct(_) => unimplemented!(),
        DataType::Union(_) => unimplemented!(),
        DataType::Dictionary(ref key_type, _) => match key_type.as_ref() {
            DataType::Int8 => {
                read_dictionary_array::<i8, _>(field_nodes, data_type, buffers, reader)
            }
            DataType::Int16 => {
                read_dictionary_array::<i16, _>(field_nodes, data_type, buffers, reader)
            }
            DataType::Int32 => {
                read_dictionary_array::<i32, _>(field_nodes, data_type, buffers, reader)
            }
            DataType::Int64 => {
                read_dictionary_array::<i64, _>(field_nodes, data_type, buffers, reader)
            }
            DataType::UInt8 => {
                read_dictionary_array::<u8, _>(field_nodes, data_type, buffers, reader)
            }
            DataType::UInt16 => {
                read_dictionary_array::<u16, _>(field_nodes, data_type, buffers, reader)
            }
            DataType::UInt32 => {
                read_dictionary_array::<u32, _>(field_nodes, data_type, buffers, reader)
            }
            DataType::UInt64 => {
                read_dictionary_array::<u64, _>(field_nodes, data_type, buffers, reader)
            }
            _ => unreachable!(),
        },
        DataType::Decimal(_, _) => unimplemented!(),
    }
}
