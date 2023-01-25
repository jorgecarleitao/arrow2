use super::*;

/// Returns an Array memory mapped from a slice of primitive data.
///
/// # Safety
/// The lifetime of the array is bounded to the lifetime of the slice.
pub unsafe fn mmap_slice<T: NativeType>(data: &[T]) -> Result<Box<dyn Array>> {
    let num_rows = data.len();
    let null_count = 0;
    let validity = None;

    let ptr = data.as_ptr() as *const u8;
    let data = Arc::new(std::slice::from_raw_parts(
        ptr,
        std::mem::size_of::<T>() * data.len(),
    ));

    let array = create_array(
        data,
        num_rows,
        null_count,
        [validity, Some(ptr)].into_iter(),
        [].into_iter(),
        None,
    );
    unsafe { try_from(InternalArrowArray::new(array, T::PRIMITIVE.into())) }
}
