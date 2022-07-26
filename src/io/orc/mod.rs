//! APIs to read from [ORC format](https://orc.apache.org).
pub mod read;

pub use orc_format as format;

use crate::error::Error;

impl From<format::Error> for Error {
    fn from(error: format::Error) -> Self {
        Error::ExternalFormat(format!("{:?}", error))
    }
}
