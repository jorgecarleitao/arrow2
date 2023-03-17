//! Contains [`Buffer`], an immutable container for all Arrow physical types (e.g. i32, f64).

mod immutable;
mod iterator;

use crate::ffi::InternalArrowArray;

pub(crate) enum BytesAllocator {
    InternalArrowArray(InternalArrowArray),

    #[cfg(feature = "arrow")]
    Arrow(arrow_buffer::Buffer),
}

pub(crate) type Bytes<T> = foreign_vec::ForeignVec<BytesAllocator, T>;

#[cfg(feature = "arrow")]
pub(crate) fn to_buffer<T: crate::types::NativeType>(
    value: std::sync::Arc<Bytes<T>>,
) -> arrow_buffer::Buffer {
    // This should never panic as ForeignVec pointer must be non-null
    let ptr = std::ptr::NonNull::new(value.as_ptr() as _).unwrap();
    let len = value.len() * std::mem::size_of::<T>();
    // Safety: allocation is guaranteed to be valid for `len` bytes
    unsafe { arrow_buffer::Buffer::from_custom_allocation(ptr, len, value) }
}

#[cfg(feature = "arrow")]
pub(crate) fn to_bytes<T: crate::types::NativeType>(value: arrow_buffer::Buffer) -> Bytes<T> {
    let ptr = value.as_ptr();
    let align = ptr.align_offset(std::mem::align_of::<T>());
    assert_eq!(align, 0, "not aligned");
    let len = value.len() / std::mem::size_of::<T>();

    // Valid as `NativeType: Pod` and checked alignment above
    let ptr = value.as_ptr() as *const T;

    let owner = crate::buffer::BytesAllocator::Arrow(value);

    // Safety: slice is valid for len elements of T
    unsafe { Bytes::from_foreign(ptr, len, owner) }
}

pub(super) use iterator::IntoIter;

pub use immutable::Buffer;
