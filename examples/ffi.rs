use arrow2::array::{Array, PrimitiveArray};
use arrow2::datatypes::Field;
use arrow2::error::Result;
use arrow2::ffi;
use std::sync::Arc;

unsafe fn export(
    array: Arc<dyn Array>,
    array_ptr: *mut ffi::Ffi_ArrowArray,
    schema_ptr: *mut ffi::Ffi_ArrowSchema,
) {
    let field = Field::new("a", array.data_type().clone(), true);
    ffi::export_array_to_c(array, array_ptr);
    ffi::export_field_to_c(&field, schema_ptr);
}

unsafe fn import(
    array: Box<ffi::Ffi_ArrowArray>,
    schema: &ffi::Ffi_ArrowSchema,
) -> Result<Box<dyn Array>> {
    let field = ffi::import_field_from_c(schema)?;
    ffi::import_array_from_c(array, &field)
}

fn main() -> Result<()> {
    // let's assume that we have an array:
    let array = Arc::new(PrimitiveArray::<i32>::from([Some(1), None, Some(123)])) as Arc<dyn Array>;

    // the goal is to export this array and import it back via FFI.
    // to import, we initialize the structs that will receive the data
    let array_ptr = Box::new(ffi::Ffi_ArrowArray::empty());
    let schema_ptr = Box::new(ffi::Ffi_ArrowSchema::empty());

    // since FFIs work in raw pointers, let's temporarily relinquish ownership so that producers
    // can write into it in a thread-safe manner
    let array_ptr = Box::into_raw(array_ptr);
    let schema_ptr = Box::into_raw(schema_ptr);

    // this is where a producer (in this case also us ^_^) writes to the pointers' location.
    // `array` here could be anything or not even be available, if this was e.g. from Python.
    // Safety: we just allocated the pointers correctly.
    unsafe { export(array.clone(), array_ptr, schema_ptr) };

    // we can now take ownership back, since we are responsible for deallocating this memory.
    // Safety: we just into_raw them.
    let array_ptr = unsafe { Box::from_raw(array_ptr) };
    let schema_ptr = unsafe { Box::from_raw(schema_ptr) };

    // and finally interpret the written memory into a new array.
    // Safety: we used `export`, which is a valid exporter to the C data interface
    let new_array = unsafe { import(array_ptr, schema_ptr.as_ref())? };

    // which is equal to the exported array
    assert_eq!(array.as_ref(), new_array.as_ref());
    Ok(())
}
