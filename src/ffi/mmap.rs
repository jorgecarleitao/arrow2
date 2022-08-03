use std::collections::VecDeque;

use crate::array::{Array, FixedSizeListArray, ListArray, Offset, StructArray};
use crate::datatypes::DataType;
use crate::error::Error;

use crate::io::ipc::read::OutOfSpecKind;
use crate::io::ipc::read::{IpcBuffer, Node};
use crate::types::NativeType;

use super::{try_from, ArrowArray, InternalArrowArray};

#[allow(dead_code)]
struct PrivateData<T> {
    // the owner of the pointers' regions
    data: T,
    buffers_ptr: Box<[*const std::os::raw::c_void]>,
    children_ptr: Box<[*mut ArrowArray]>,
    dictionary_ptr: Option<*mut ArrowArray>,
}

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

fn create_array<
    T: Clone + AsRef<[u8]>,
    I: Iterator<Item = Option<*const u8>>,
    II: Iterator<Item = ArrowArray>,
>(
    data: T,
    num_rows: usize,
    null_count: usize,
    buffers: I,
    children: II,
) -> ArrowArray {
    let buffers_ptr = buffers
        .map(|maybe_buffer| match maybe_buffer {
            Some(b) => b as *const std::os::raw::c_void,
            None => std::ptr::null(),
        })
        .collect::<Box<[_]>>();
    let n_buffers = buffers_ptr.len() as i64;

    let children_ptr = children
        .map(|child| Box::into_raw(Box::new(child)))
        .collect::<Box<_>>();
    let n_children = children_ptr.len() as i64;

    let mut private_data = Box::new(PrivateData::<T> {
        data,
        buffers_ptr,
        children_ptr,
        dictionary_ptr: None,
    });

    ArrowArray {
        length: num_rows as i64,
        null_count: null_count as i64,
        offset: 0, // IPC files are by definition not offset
        n_buffers,
        n_children,
        buffers: private_data.buffers_ptr.as_mut_ptr(),
        children: private_data.children_ptr.as_mut_ptr(),
        dictionary: private_data.dictionary_ptr.unwrap_or(std::ptr::null_mut()),
        release: Some(release::<T>),
        private_data: Box::into_raw(private_data) as *mut ::std::os::raw::c_void,
    }
}

/// callback used to drop [`ArrowArray`] when it is exported
unsafe extern "C" fn release<T>(array: *mut ArrowArray) {
    if array.is_null() {
        return;
    }
    let array = &mut *array;

    // take ownership of `private_data`, therefore dropping it
    let private = Box::from_raw(array.private_data as *mut PrivateData<T>);
    for child in private.children_ptr.iter() {
        let _ = Box::from_raw(*child);
    }

    if let Some(ptr) = private.dictionary_ptr {
        let _ = Box::from_raw(ptr);
    }

    array.release = None;
}

fn mmap_binary<O: Offset, T: Clone + AsRef<[u8]>>(
    data: T,
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

    let data_ref = data.as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let offsets = get_buffer::<O>(data_ref, block_offset, buffers, num_rows + 1)?.as_ptr();
    let values = get_buffer::<u8>(data_ref, block_offset, buffers, 0)?.as_ptr();

    // NOTE: offsets and values invariants are _not_ validated
    Ok(create_array(
        data,
        num_rows,
        null_count,
        [validity, Some(offsets), Some(values)].into_iter(),
        [].into_iter(),
    ))
}

fn mmap_fixed_size_binary<T: Clone + AsRef<[u8]>>(
    data: T,
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

    let data_ref = data.as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let values = get_buffer::<u8>(data_ref, block_offset, buffers, num_rows + 1)?.as_ptr();

    Ok(create_array(
        data,
        num_rows,
        null_count,
        [validity, Some(values)].into_iter(),
        [].into_iter(),
    ))
}

fn mmap_null<T: Clone + AsRef<[u8]>>(
    data: T,
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

    Ok(create_array(
        data,
        num_rows,
        null_count,
        [].into_iter(),
        [].into_iter(),
    ))
}

fn mmap_boolean<T: Clone + AsRef<[u8]>>(
    data: T,
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

    let data_ref = data.as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let values = get_buffer_bounds(buffers)?;
    let (offset, length) = values;

    // verify that they are in-bounds and get its pointer
    let values = data_ref[block_offset + offset..block_offset + offset + length].as_ptr();

    Ok(create_array(
        data,
        num_rows,
        null_count,
        [validity, Some(values)].into_iter(),
        [].into_iter(),
    ))
}

fn mmap_primitive<P: NativeType, T: Clone + AsRef<[u8]>>(
    data: T,
    node: &Node,
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<ArrowArray, Error> {
    let data_ref = data.as_ref();

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

    Ok(create_array(
        data,
        num_rows,
        null_count,
        [validity, Some(values)].into_iter(),
        [].into_iter(),
    ))
}

fn mmap_list<O: Offset, T: Clone + AsRef<[u8]>>(
    data: T,
    node: &Node,
    block_offset: usize,
    data_type: &DataType,
    field_nodes: &mut VecDeque<Node>,
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

    let data_ref = data.as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let offsets = get_buffer::<O>(data_ref, block_offset, buffers, num_rows + 1)?.as_ptr();

    let values = get_array(data.clone(), block_offset, child, field_nodes, buffers)?;

    // NOTE: offsets and values invariants are _not_ validated
    Ok(create_array(
        data,
        num_rows,
        null_count,
        [validity, Some(offsets)].into_iter(),
        [values].into_iter(),
    ))
}

fn mmap_fixed_size_list<T: Clone + AsRef<[u8]>>(
    data: T,
    node: &Node,
    block_offset: usize,
    data_type: &DataType,
    field_nodes: &mut VecDeque<Node>,
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

    let data_ref = data.as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let values = get_array(data.clone(), block_offset, child, field_nodes, buffers)?;

    Ok(create_array(
        data,
        num_rows,
        null_count,
        [validity].into_iter(),
        [values].into_iter(),
    ))
}

fn mmap_struct<T: Clone + AsRef<[u8]>>(
    data: T,
    node: &Node,
    block_offset: usize,
    data_type: &DataType,
    field_nodes: &mut VecDeque<Node>,
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

    let data_ref = data.as_ref();

    let validity = get_validity(data_ref, block_offset, buffers, null_count)?.map(|x| x.as_ptr());

    let values = children
        .iter()
        .map(|f| &f.data_type)
        .map(|child| get_array(data.clone(), block_offset, child, field_nodes, buffers))
        .collect::<Result<Vec<_>, Error>>()?;

    Ok(create_array(
        data,
        num_rows,
        null_count,
        [validity].into_iter(),
        values.into_iter(),
    ))
}

fn get_array<T: Clone + AsRef<[u8]>>(
    data: T,
    block_offset: usize,
    data_type: &DataType,
    field_nodes: &mut VecDeque<Node>,
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
        FixedSizeBinary => mmap_fixed_size_binary(data, &node, block_offset, buffers),
        LargeBinary | LargeUtf8 => mmap_binary::<i64, _>(data, &node, block_offset, buffers),
        List => mmap_list::<i32, _>(data, &node, block_offset, data_type, field_nodes, buffers),
        LargeList => {
            mmap_list::<i64, _>(data, &node, block_offset, data_type, field_nodes, buffers)
        }
        FixedSizeList => {
            mmap_fixed_size_list(data, &node, block_offset, data_type, field_nodes, buffers)
        }
        Struct => mmap_struct(data, &node, block_offset, data_type, field_nodes, buffers),

        _ => todo!(),
    }
}

/// Maps a memory region to an [`Array`].
pub(crate) unsafe fn mmap<T: Clone + AsRef<[u8]>>(
    data: T,
    block_offset: usize,
    data_type: DataType,
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<Box<dyn Array>, Error> {
    let array = get_array(data, block_offset, &data_type, field_nodes, buffers)?;
    // The unsafety comes from the fact that `array` is not necessarily valid -
    // the IPC file may be corrupted (e.g. invalid offsets or non-utf8 data)
    unsafe { try_from(InternalArrowArray::new(array, data_type)) }
}
