use std::ffi::CStr;

use crate::{array::Array, datatypes::Field, error::ArrowError};

use super::{import_array_from_c, import_field_from_c};
use super::{ArrowArray, ArrowArrayStream, ArrowSchema};

impl Drop for ArrowArrayStream {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

impl ArrowArrayStream {
    /// Creates an empty [`ArrowArrayStream`] used to import from a producer.
    pub fn empty() -> Self {
        Self {
            get_schema: None,
            get_next: None,
            get_last_error: None,
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }
}

unsafe fn handle_error(iter: &mut ArrowArrayStream) -> ArrowError {
    let error = unsafe { (iter.get_last_error.unwrap())(&mut *iter) };

    if error.is_null() {
        return ArrowError::External(
            "C stream".to_string(),
            Box::new(ArrowError::ExternalFormat(
                "an unspecified error".to_string(),
            )),
        );
    }

    let error = unsafe { CStr::from_ptr(error) };
    ArrowError::External(
        "C stream".to_string(),
        Box::new(ArrowError::ExternalFormat(
            error.to_str().unwrap().to_string(),
        )),
    )
}

/// Interface for the Arrow C stream interface. Implements an iterator of [`Array`].
///
pub struct ArrowArrayStreamReader {
    iter: Box<ArrowArrayStream>,
    field: Field,
}

impl ArrowArrayStreamReader {
    /// Returns a new [`ArrowArrayStreamReader`]
    /// # Error
    /// Errors iff the [`ArrowArrayStream`] is out of specification
    /// # Safety
    /// This method is intrinsically `unsafe` since it assumes that the `ArrowArrayStream`
    /// contains a valid Arrow C stream interface.
    /// In particular:
    /// * The `ArrowArrayStream` fulfills the invariants of the C stream interface
    /// * The schema `get_schema` produces fulfills the C data interface
    pub unsafe fn try_new(mut iter: Box<ArrowArrayStream>) -> Result<Self, ArrowError> {
        let mut field = Box::new(ArrowSchema::empty());

        if iter.get_next.is_none() {
            return Err(ArrowError::OutOfSpec(
                "The C stream MUST contain a non-null get_next".to_string(),
            ));
        };

        if iter.get_last_error.is_none() {
            return Err(ArrowError::OutOfSpec(
                "The C stream MUST contain a non-null get_last_error".to_string(),
            ));
        };

        let status = if let Some(f) = iter.get_schema {
            unsafe { (f)(&mut *iter, &mut *field) }
        } else {
            return Err(ArrowError::OutOfSpec(
                "The C stream MUST contain a non-null get_schema".to_string(),
            ));
        };

        if status != 0 {
            return Err(unsafe { handle_error(&mut iter) });
        }

        let field = unsafe { import_field_from_c(&field)? };

        Ok(Self { iter, field })
    }

    /// Returns the field provided by the stream
    pub fn field(&self) -> &Field {
        &self.field
    }

    /// Advances this iterator by one array
    /// # Error
    /// Errors iff:
    /// * The C stream interface returns an error
    /// * The C stream interface returns an invalid array (that we can identify, see Safety below)
    /// # Safety
    /// Calling this iterator's `next` assumes that the [`ArrowArrayStream`] produces arrow arrays
    /// that fulfill the C data interface
    pub unsafe fn next(&mut self) -> Option<Result<Box<dyn Array>, ArrowError>> {
        let mut array = Box::new(ArrowArray::empty());
        let status = unsafe { (self.iter.get_next.unwrap())(&mut *self.iter, &mut *array) };

        if status != 0 {
            return Some(Err(unsafe { handle_error(&mut self.iter) }));
        }

        // last paragraph of https://arrow.apache.org/docs/format/CStreamInterface.html#c.ArrowArrayStream.get_next
        array.release?;

        // Safety: assumed from the C stream interface
        unsafe { import_array_from_c(array, self.field.data_type.clone()) }
            .map(Some)
            .transpose()
    }
}
