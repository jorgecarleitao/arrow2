use std::collections::VecDeque;
use std::sync::Arc;

use crate::array::{Array, DictionaryKey, FixedSizeListArray, ListArray, StructArray, View};
use crate::datatypes::DataType;
use crate::error::Error;
use crate::offset::Offset;

use crate::io::ipc::read::{Dictionaries, OutOfSpecKind};
use crate::io::ipc::read::{IpcBuffer, Node};
use crate::io::ipc::IpcField;
use crate::types::NativeType;

use crate::ffi::mmap::create_array;
use crate::ffi::{export_array_to_c, try_from, ArrowArray, InternalArrowArray};

fn get_buffer_bounds(buffers: &mut VecDeque<IpcBuffer>) -> Result<(usize, usize), Error> {
    let buffer = buffers
        .pop_front()
        .ok_or_else(|| Error::from(OutOfSpecKind::ExpectedBuffer))?;

    let offset: usize = buffer
        .offset()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let length: usize = buffer
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    Ok((offset, length))
}

fn get_buffer<'a, T: NativeType>(
    data: &'a [u8],
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
    num_rows: usize,
) -> Result<&'a [u8], Error> {
    let (offset, length) = get_buffer_bounds(buffers)?;

    // verify that they are in-bounds
    let values = data
        .get(block_offset + offset..block_offset + offset + length)
        .ok_or_else(|| Error::OutOfSpec("buffer out of bounds".to_string()))?;

    // validate alignment
    let v: &[T] = bytemuck::try_cast_slice(values)
        .map_err(|_| Error::OutOfSpec("buffer not aligned for mmap".to_string()))?;

    if v.len() < num_rows {
        return Err(Error::OutOfSpec(
            "buffer's length is too small in mmap".to_string(),
        ));
    }

    Ok(values)
}

fn get_bytes<'a>(
    data: &'a [u8],
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<&'a [u8], Error> {
    let (offset, length) = get_buffer_bounds(buffers)?;

    // verify that they are in-bounds
    data.get(block_offset + offset..block_offset + offset + length)
        .ok_or_else(|| Error::OutOfSpec("buffer out of bounds".to_string()))
}

fn get_validity<'a>(
    data: &'a [u8],
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
    null_count: usize,
) -> Result<Option<&'a [u8]>, Error> {
    let validity = get_buffer_bounds(buffers)?;
    let (offset, length) = validity;

    Ok(if null_count > 0 {
        // verify that they are in-bounds and get its pointer
        Some(
            data.get(block_offset + offset..block_offset + offset + length)
                .ok_or_else(|| Error::OutOfSpec("buffer out of bounds".to_string()))?,
        )
    } else {
        None
    })
}
fn get_num_rows_and_null_count(node: &Node) -> Result<(usize, usize), Error> {
    let num_rows: usize = node
        .length()
        .try_into()
        .map_err(|_| Error::OutOfSpec("Negative footer length".to_string()))?;

    let null_count: usize = node
        .null_count()
        .try_into()
        .map_err(|_|  Error::OutOfSpec("Negative footer length".to_string()))?;
    Ok((num_rows, null_count))
}

fn mmap_binary<O: Offset, T: AsRef<[u8]>>(
    data: Arc<T>,
    node: &Node,
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<ArrowArray, Error> {
    let num_rows: usize = node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let null_count: usize = node
        .null_count()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let data_ref = data.as_ref().as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let offsets = get_buffer::<O>(data_ref, block_offset, buffers, num_rows + 1)?.as_ptr();
    let values = get_buffer::<u8>(data_ref, block_offset, buffers, 0)?.as_ptr();

    // NOTE: offsets and values invariants are _not_ validated
    Ok(unsafe {
        create_array(
            data,
            num_rows,
            null_count,
            [validity, Some(offsets), Some(values)].into_iter(),
            [].into_iter(),
            None,
            None,
        )
    })
}

fn mmap_binview<T: AsRef<[u8]>>(
    data: Arc<T>,
    node: &Node,
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
    variadic_buffer_counts: &mut VecDeque<usize>,
) -> Result<ArrowArray, Error> {
    let (num_rows, null_count) = get_num_rows_and_null_count(node)?;
    let data_ref = data.as_ref().as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let views = get_buffer::<View>(data_ref, block_offset, buffers, num_rows)?;

    let n_variadic = variadic_buffer_counts
        .pop_front()
        .ok_or_else(|| Error::OutOfSpec("expected variadic_buffer_count".to_string()))?;

    let mut buffer_ptrs = Vec::with_capacity(n_variadic + 2);
    buffer_ptrs.push(validity);
    buffer_ptrs.push(Some(views.as_ptr()));

    let mut variadic_buffer_sizes = Vec::with_capacity(n_variadic);
    for _ in 0..n_variadic {
        let variadic_buffer = get_bytes(data_ref, block_offset, buffers)?;
        variadic_buffer_sizes.push(variadic_buffer.len() as i64);
        buffer_ptrs.push(Some(variadic_buffer.as_ptr()));
    }
    buffer_ptrs.push(Some(variadic_buffer_sizes.as_ptr().cast::<u8>()));

    // Move variadic buffer sizes in an Arc, so that it stays alive.
    let data = Arc::new((data, variadic_buffer_sizes));

    // NOTE: invariants are not validated
    Ok(unsafe {
        create_array(
            data,
            num_rows,
            null_count,
            buffer_ptrs.into_iter(),
            [].into_iter(),
            None,
            None,
        )
    })
}

fn mmap_fixed_size_binary<T: AsRef<[u8]>>(
    data: Arc<T>,
    node: &Node,
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
    data_type: &DataType,
) -> Result<ArrowArray, Error> {
    let bytes_per_row = if let DataType::FixedSizeBinary(bytes_per_row) = data_type {
        bytes_per_row
    } else {
        return Err(Error::from(OutOfSpecKind::InvalidDataType));
    };

    let num_rows: usize = node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let null_count: usize = node
        .null_count()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let data_ref = data.as_ref().as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());
    let values =
        get_buffer::<u8>(data_ref, block_offset, buffers, num_rows * bytes_per_row)?.as_ptr();

    Ok(unsafe {
        create_array(
            data,
            num_rows,
            null_count,
            [validity, Some(values)].into_iter(),
            [].into_iter(),
            None,
            None,
        )
    })
}

fn mmap_null<T: AsRef<[u8]>>(
    data: Arc<T>,
    node: &Node,
    _block_offset: usize,
    _buffers: &mut VecDeque<IpcBuffer>,
) -> Result<ArrowArray, Error> {
    let num_rows: usize = node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let null_count: usize = node
        .null_count()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    Ok(unsafe {
        create_array(
            data,
            num_rows,
            null_count,
            [].into_iter(),
            [].into_iter(),
            None,
            None,
        )
    })
}

fn mmap_boolean<T: AsRef<[u8]>>(
    data: Arc<T>,
    node: &Node,
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<ArrowArray, Error> {
    let num_rows: usize = node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let null_count: usize = node
        .null_count()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let data_ref = data.as_ref().as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let values = get_buffer_bounds(buffers)?;
    let (offset, length) = values;

    // verify that they are in-bounds and get its pointer
    let values = data_ref[block_offset + offset..block_offset + offset + length].as_ptr();

    Ok(unsafe {
        create_array(
            data,
            num_rows,
            null_count,
            [validity, Some(values)].into_iter(),
            [].into_iter(),
            None,
            None,
        )
    })
}

fn mmap_primitive<P: NativeType, T: AsRef<[u8]>>(
    data: Arc<T>,
    node: &Node,
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<ArrowArray, Error> {
    let data_ref = data.as_ref().as_ref();

    let num_rows: usize = node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let null_count: usize = node
        .null_count()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let values = get_buffer::<P>(data_ref, block_offset, buffers, num_rows)?.as_ptr();

    Ok(unsafe {
        create_array(
            data,
            num_rows,
            null_count,
            [validity, Some(values)].into_iter(),
            [].into_iter(),
            None,
            None,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn mmap_list<O: Offset, T: AsRef<[u8]>>(
    data: Arc<T>,
    node: &Node,
    block_offset: usize,
    data_type: &DataType,
    ipc_field: &IpcField,
    dictionaries: &Dictionaries,
    field_nodes: &mut VecDeque<Node>,
    variadic_buffer_counts: &mut VecDeque<usize>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<ArrowArray, Error> {
    let child = ListArray::<O>::try_get_child(data_type)?.data_type();

    let num_rows: usize = node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let null_count: usize = node
        .null_count()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let data_ref = data.as_ref().as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let offsets = get_buffer::<O>(data_ref, block_offset, buffers, num_rows + 1)?.as_ptr();

    let values = get_array(
        data.clone(),
        block_offset,
        child,
        &ipc_field.fields[0],
        dictionaries,
        field_nodes,
        variadic_buffer_counts,
        buffers,
    )?;

    // NOTE: offsets and values invariants are _not_ validated
    Ok(unsafe {
        create_array(
            data,
            num_rows,
            null_count,
            [validity, Some(offsets)].into_iter(),
            [values].into_iter(),
            None,
            None,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn mmap_fixed_size_list<T: AsRef<[u8]>>(
    data: Arc<T>,
    node: &Node,
    block_offset: usize,
    data_type: &DataType,
    ipc_field: &IpcField,
    dictionaries: &Dictionaries,
    field_nodes: &mut VecDeque<Node>,
    variadic_buffer_counts: &mut VecDeque<usize>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<ArrowArray, Error> {
    let child = FixedSizeListArray::try_child_and_size(data_type)?
        .0
        .data_type();

    let num_rows: usize = node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let null_count: usize = node
        .null_count()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let data_ref = data.as_ref().as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let values = get_array(
        data.clone(),
        block_offset,
        child,
        &ipc_field.fields[0],
        dictionaries,
        field_nodes,
        variadic_buffer_counts,
        buffers,
    )?;

    Ok(unsafe {
        create_array(
            data,
            num_rows,
            null_count,
            [validity].into_iter(),
            [values].into_iter(),
            None,
            None,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn mmap_struct<T: AsRef<[u8]>>(
    data: Arc<T>,
    node: &Node,
    block_offset: usize,
    data_type: &DataType,
    ipc_field: &IpcField,
    dictionaries: &Dictionaries,
    field_nodes: &mut VecDeque<Node>,
    variadic_buffer_counts: &mut VecDeque<usize>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<ArrowArray, Error> {
    let children = StructArray::try_get_fields(data_type)?;

    let num_rows: usize = node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let null_count: usize = node
        .null_count()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let data_ref = data.as_ref().as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let values = children
        .iter()
        .map(|f| &f.data_type)
        .zip(ipc_field.fields.iter())
        .map(|(child, ipc)| {
            get_array(
                data.clone(),
                block_offset,
                child,
                ipc,
                dictionaries,
                field_nodes,
                variadic_buffer_counts,
                buffers,
            )
        })
        .collect::<Result<Vec<_>, Error>>()?;

    Ok(unsafe {
        create_array(
            data,
            num_rows,
            null_count,
            [validity].into_iter(),
            values.into_iter(),
            None,
            None,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn mmap_dict<K: DictionaryKey, T: AsRef<[u8]>>(
    data: Arc<T>,
    node: &Node,
    block_offset: usize,
    _: &DataType,
    ipc_field: &IpcField,
    dictionaries: &Dictionaries,
    _: &mut VecDeque<Node>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<ArrowArray, Error> {
    let num_rows: usize = node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let null_count: usize = node
        .null_count()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let data_ref = data.as_ref().as_ref();

    let dictionary = dictionaries
        .get(&ipc_field.dictionary_id.unwrap())
        .ok_or_else(|| Error::oos("Missing dictionary"))?
        .clone();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let values = get_buffer::<K>(data_ref, block_offset, buffers, num_rows)?.as_ptr();

    Ok(unsafe {
        create_array(
            data,
            num_rows,
            null_count,
            [validity, Some(values)].into_iter(),
            [].into_iter(),
            Some(export_array_to_c(dictionary)),
            None,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn get_array<T: AsRef<[u8]>>(
    data: Arc<T>,
    block_offset: usize,
    data_type: &DataType,
    ipc_field: &IpcField,
    dictionaries: &Dictionaries,
    field_nodes: &mut VecDeque<Node>,
    variadic_buffer_counts: &mut VecDeque<usize>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<ArrowArray, Error> {
    use crate::datatypes::PhysicalType::*;
    let node = field_nodes
        .pop_front()
        .ok_or_else(|| Error::from(OutOfSpecKind::ExpectedBuffer))?;

    match data_type.to_physical_type() {
        Null => mmap_null(data, &node, block_offset, buffers),
        Boolean => mmap_boolean(data, &node, block_offset, buffers),
        Primitive(p) => with_match_primitive_type!(p, |$T| {
            mmap_primitive::<$T, _>(data, &node, block_offset, buffers)
        }),
        Utf8 | Binary => mmap_binary::<i32, _>(data, &node, block_offset, buffers),
        Utf8View | BinaryView => {
            mmap_binview(data, &node, block_offset, buffers, variadic_buffer_counts)
        },
        FixedSizeBinary => mmap_fixed_size_binary(data, &node, block_offset, buffers, data_type),
        LargeBinary | LargeUtf8 => mmap_binary::<i64, _>(data, &node, block_offset, buffers),
        List => mmap_list::<i32, _>(
            data,
            &node,
            block_offset,
            data_type,
            ipc_field,
            dictionaries,
            field_nodes,
            variadic_buffer_counts,
            buffers,
        ),
        LargeList => mmap_list::<i64, _>(
            data,
            &node,
            block_offset,
            data_type,
            ipc_field,
            dictionaries,
            field_nodes,
            variadic_buffer_counts,
            buffers,
        ),
        FixedSizeList => mmap_fixed_size_list(
            data,
            &node,
            block_offset,
            data_type,
            ipc_field,
            dictionaries,
            field_nodes,
            variadic_buffer_counts,
            buffers,
        ),
        Struct => mmap_struct(
            data,
            &node,
            block_offset,
            data_type,
            ipc_field,
            dictionaries,
            field_nodes,
            variadic_buffer_counts,
            buffers,
        ),
        Dictionary(key_type) => match_integer_type!(key_type, |$T| {
            mmap_dict::<$T, _>(
                data,
                &node,
                block_offset,
                data_type,
                ipc_field,
                dictionaries,
                field_nodes,
                buffers,
            )
        }),
        _ => todo!(),
    }
}

#[allow(clippy::too_many_arguments)]
/// Maps a memory region to an [`Array`].
pub(crate) unsafe fn mmap<T: AsRef<[u8]>>(
    data: Arc<T>,
    block_offset: usize,
    data_type: DataType,
    ipc_field: &IpcField,
    dictionaries: &Dictionaries,
    field_nodes: &mut VecDeque<Node>,
    variadic_buffer_counts: &mut VecDeque<usize>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<Box<dyn Array>, Error> {
    let array = get_array(
        data,
        block_offset,
        &data_type,
        ipc_field,
        dictionaries,
        field_nodes,
        variadic_buffer_counts,
        buffers,
    )?;
    // The unsafety comes from the fact that `array` is not necessarily valid -
    // the IPC file may be corrupted (e.g. invalid offsets or non-utf8 data)
    unsafe { try_from(InternalArrowArray::new(array, data_type)) }
}
