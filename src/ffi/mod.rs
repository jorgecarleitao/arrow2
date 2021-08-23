//! Contains interfaces to use the
//! [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
mod array;
#[allow(clippy::module_inception)]
mod ffi;
mod schema;

pub use array::try_from;
pub use ffi::{ArrowArray, ArrowArrayRef};

use std::sync::Arc;

use crate::array::Array;
use crate::datatypes::Field;
use crate::error::Result;

use ffi::*;
use schema::Ffi_ArrowSchema;

use self::schema::to_field;

/// Exports an `Array` to the C data interface.
pub fn export_array_to_c(array: Arc<dyn Array>) -> Arc<Ffi_ArrowArray> {
    Arc::new(Ffi_ArrowArray::new(array))
}

/// Exports a [`Field`] to the C data interface.
pub fn export_field_to_c(field: &Field) -> Arc<Ffi_ArrowSchema> {
    Arc::new(Ffi_ArrowSchema::new(field))
}

/// Imports a [`Field`] from the C data interface.
pub fn import_field_from_c(field: &Ffi_ArrowSchema) -> Result<Field> {
    to_field(field)
}

/// Imports a [`Field`] from the C data interface.
pub fn import_array_from_c(array: Arc<Ffi_ArrowArray>, field: &Field) -> Result<Box<dyn Array>> {
    try_from(Arc::new(ArrowArray::new(array, field.clone())))
}
