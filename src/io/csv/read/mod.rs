mod parser;
mod reader;

// Re-export for usage by consumers.
pub use csv::{ByteRecord, Reader, ReaderBuilder};

mod infer_schema;
mod read_boolean;
mod read_primitive;
mod read_utf8;

pub use infer_schema::infer_schema;
pub use parser::DefaultParser;
pub use read_boolean::{new_boolean_array, BooleanParser};
pub use read_primitive::{new_primitive_array, PrimitiveParser};
pub use read_utf8::{new_utf8_array, Utf8Parser};
pub use reader::*;
