//! contains FFI bindings to import and export [`Array`](crate::array::Array) via
//! Arrow's [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
mod array;
#[allow(clippy::module_inception)]
mod ffi;
mod schema;

pub(crate) use array::try_from;
pub(crate) use ffi::{ArrowArray, ArrowArrayRef};

use std::sync::Arc;

use crate::array::Array;
use crate::datatypes::Field;
use crate::error::Result;

pub use ffi::Ffi_ArrowArray;
pub use schema::Ffi_ArrowSchema;

use self::schema::to_field;

/// Exports an [`Arc<dyn Array>`] to the C data interface.
/// # Safety
/// The pointer `ptr` must be allocated and valid
pub unsafe fn export_array_to_c(array: Arc<dyn Array>, ptr: *mut Ffi_ArrowArray) {
    *ptr = Ffi_ArrowArray::new(array);
}

/// Exports a [`Field`] to the C data interface.
/// # Safety
/// The pointer `ptr` must be allocated and valid
pub unsafe fn export_field_to_c(field: &Field, ptr: *mut Ffi_ArrowSchema) {
    *ptr = Ffi_ArrowSchema::new(field)
}

/// Imports a [`Field`] from the C data interface.
/// # Safety
/// This function is intrinsically `unsafe` and relies on a [`Ffi_ArrowSchema`]
/// valid according to the [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html) (FFI).
pub unsafe fn import_field_from_c(field: &Ffi_ArrowSchema) -> Result<Field> {
    to_field(field)
}

/// Imports an [`Array`] from the C data interface.
/// # Safety
/// This function is intrinsically `unsafe` and relies on a [`Ffi_ArrowArray`]
/// valid according to the [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html) (FFI).
pub unsafe fn import_array_from_c(
    array: Box<Ffi_ArrowArray>,
    field: &Field,
) -> Result<Box<dyn Array>> {
    try_from(Arc::new(ArrowArray::new(array, field.clone())))
}
