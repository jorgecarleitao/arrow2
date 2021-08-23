//! Contains interfaces to use the
//! [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
mod array;
#[allow(clippy::module_inception)]
mod ffi;
mod schema;

pub use array::try_from;
pub use ffi::{
    create_empty, export_array_to_c, export_field_to_c, import_field_from_c, ArrowArray,
    ArrowArrayRef,
};
