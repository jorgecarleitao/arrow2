use std::collections::VecDeque;

use crate::array::{Array, BooleanArray, FromFfi, Offset, Utf8Array};
use crate::datatypes::DataType;
use crate::error::Error;

use crate::io::ipc::read::OutOfSpecKind;
use crate::io::ipc::read::{IpcBuffer, Node};

use super::{ArrowArray, InternalArrowArray};

#[allow(dead_code)]
struct PrivateData<T> {
    // the owner of the pointers' regions
    data: T,
    buffers_ptr: Box<[*const std::os::raw::c_void]>,
    //children_ptr: Box<[*mut ArrowArray]>,
    dictionary_ptr: Option<*mut ArrowArray>,
}

fn get_buffer(buffers: &mut VecDeque<IpcBuffer>) -> Result<(usize, usize), Error> {
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

fn create_array<T: Clone + AsRef<[u8]>, I: Iterator<Item = Option<*const u8>>>(
    data: T,
    num_rows: usize,
    null_count: usize,
    buffers: I,
) -> ArrowArray {
    let n_buffers = buffers.size_hint().0 as i64;

    let buffers_ptr = buffers
        .map(|maybe_buffer| match maybe_buffer {
            Some(b) => b as *const std::os::raw::c_void,
            None => std::ptr::null(),
        })
        .collect::<Box<[_]>>();

    let mut private_data = Box::new(PrivateData::<T> {
        data,
        buffers_ptr,
        dictionary_ptr: None,
    });

    ArrowArray {
        length: num_rows as i64,
        null_count: null_count as i64,
        offset: 0,
        n_buffers,
        n_children: 0,
        buffers: private_data.buffers_ptr.as_mut_ptr(),
        children: std::ptr::null_mut(),
        dictionary: private_data.dictionary_ptr.unwrap_or(std::ptr::null_mut()),
        release: Some(release::<T>),
        private_data: Box::into_raw(private_data) as *mut ::std::os::raw::c_void,
    }
}

// callback used to drop [ArrowArray] when it is exported
unsafe extern "C" fn release<T>(array: *mut ArrowArray) {
    if array.is_null() {
        return;
    }
    let array = &mut *array;

    // take ownership of `private_data`, therefore dropping it
    let private = Box::from_raw(array.private_data as *mut PrivateData<T>);
    /*for child in private.children_ptr.iter() {
        let _ = Box::from_raw(*child);
    }*/

    if let Some(ptr) = private.dictionary_ptr {
        let _ = Box::from_raw(ptr);
    }

    array.release = None;
}

fn mmap_utf8<O: Offset, T: Clone + AsRef<[u8]>>(
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

    let validity = get_buffer(buffers)?;
    let (offset, length) = validity;

    let validity = if null_count > 0 {
        // verify that they are in-bounds and get its pointer
        Some(data_ref[block_offset + offset..block_offset + offset + length].as_ptr())
    } else {
        None
    };

    let offsets = get_buffer(buffers)?;
    let (offset, length) = offsets;

    // verify that they are in-bounds and get its pointer
    let offsets = &data_ref[block_offset + offset..block_offset + offset + length];

    // validate alignment
    let _: &[O] = bytemuck::cast_slice(offsets);

    let offsets = data_ref[block_offset + offset..block_offset + offset + length].as_ptr();

    let values = get_buffer(buffers)?;
    let (offset, length) = values;

    // verify that they are in-bounds and get its pointer
    let values = data_ref[block_offset + offset..block_offset + offset + length].as_ptr();

    // NOTE: offsets and values invariants are _not_ validated
    Ok(create_array(
        data,
        num_rows,
        null_count,
        [validity, Some(offsets), Some(values)].into_iter(),
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

    let validity = get_buffer(buffers)?;
    let (offset, length) = validity;

    let validity = if null_count > 0 {
        // verify that they are in-bounds and get its pointer
        Some(data_ref[block_offset + offset..block_offset + offset + length].as_ptr())
    } else {
        None
    };

    let values = get_buffer(buffers)?;
    let (offset, length) = values;

    // verify that they are in-bounds and get its pointer
    let values = data_ref[block_offset + offset..block_offset + offset + length].as_ptr();

    Ok(create_array(
        data,
        num_rows,
        null_count,
        [validity, Some(values)].into_iter(),
    ))
}

fn boolean<T: Clone + AsRef<[u8]>>(
    data: T,
    node: &Node,
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
    data_type: DataType,
) -> Result<BooleanArray, Error> {
    let array = mmap_boolean(data, node, block_offset, buffers)?;
    let array = InternalArrowArray::new(array, data_type);
    // this is safe because we just (correctly) constructed `ArrowArray`
    unsafe { BooleanArray::try_from_ffi(array) }
}

unsafe fn utf8<O: Offset, T: Clone + AsRef<[u8]>>(
    data: T,
    node: &Node,
    block_offset: usize,
    buffers: &mut VecDeque<IpcBuffer>,
    data_type: DataType,
) -> Result<Utf8Array<O>, Error> {
    let array = mmap_utf8::<O, _>(data, node, block_offset, buffers)?;
    let array = InternalArrowArray::new(array, data_type);
    // this is unsafe because `mmap_utf8` does not validate invariants
    unsafe { Utf8Array::<O>::try_from_ffi(array) }
}

/// Maps a memory region to an [`Array`].
pub(crate) unsafe fn mmap<T: Clone + AsRef<[u8]>>(
    data: T,
    block_offset: usize,
    data_type: DataType,
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<Box<dyn Array>, Error> {
    use crate::datatypes::PhysicalType::*;
    let node = field_nodes
        .pop_front()
        .ok_or_else(|| Error::from(OutOfSpecKind::ExpectedBuffer))?;
    match data_type.to_physical_type() {
        Boolean => boolean(data, &node, block_offset, buffers, data_type).map(|x| x.boxed()),
        Utf8 => utf8::<i32, _>(data, &node, block_offset, buffers, data_type).map(|x| x.boxed()),
        LargeUtf8 => {
            utf8::<i64, _>(data, &node, block_offset, buffers, data_type).map(|x| x.boxed())
        }
        _ => todo!(),
    }
}
