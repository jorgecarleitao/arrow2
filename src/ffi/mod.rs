//! Contains interfaces to use the
//! [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
mod array;
#[allow(clippy::module_inception)]
mod ffi;

pub use array::try_from;
pub use ffi::{create_empty, export_to_c, ArrowArray, ArrowArrayRef};
