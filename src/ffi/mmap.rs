//! Functionality to mmap in-memory data regions.
use std::sync::Arc;

use super::ArrowArray;

#[allow(dead_code)]
struct PrivateData<T> {
    // the owner of the pointers' regions
    data: T,
    buffers_ptr: Box<[*const std::os::raw::c_void]>,
    children_ptr: Box<[*mut ArrowArray]>,
    dictionary_ptr: Option<*mut ArrowArray>,
}

pub(crate) unsafe fn create_array<
    T: AsRef<[u8]>,
    I: Iterator<Item = Option<*const u8>>,
    II: Iterator<Item = ArrowArray>,
>(
    data: Arc<T>,
    num_rows: usize,
    null_count: usize,
    buffers: I,
    children: II,
    dictionary: Option<ArrowArray>,
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

    let dictionary_ptr = dictionary.map(|array| Box::into_raw(Box::new(array)));

    let mut private_data = Box::new(PrivateData::<Arc<T>> {
        data,
        buffers_ptr,
        children_ptr,
        dictionary_ptr,
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
        release: Some(release::<Arc<T>>),
        private_data: Box::into_raw(private_data) as *mut ::std::os::raw::c_void,
    }
}

/// callback used to drop [`ArrowArray`] when it is exported specified for [`PrivateData`].
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
