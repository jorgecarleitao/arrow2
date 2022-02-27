//! APIs to read and deserialize from JSON
mod deserialize;
mod infer_schema;

pub(crate) use deserialize::_deserialize;
pub use deserialize::deserialize;
pub(crate) use infer_schema::coerce_data_type;
pub use infer_schema::infer;
