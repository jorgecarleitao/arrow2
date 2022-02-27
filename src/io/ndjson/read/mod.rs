//! APIs to read and deserialize [NDJSON](http://ndjson.org/).

pub use fallible_streaming_iterator::FallibleStreamingIterator;

mod deserialize;
mod file;
pub use deserialize::deserialize;
pub use file::{infer, FileReader};
