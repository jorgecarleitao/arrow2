//! This library demonstrates a minimal usage of Rust's C data interface to pass
//! arrays from and to Python.
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;

use arrow2::ffi;

use super::*;

pub fn to_rust_iterator(ob: PyObject, py: Python) -> PyResult<Vec<PyObject>> {
    let stream = Box::new(ffi::ArrowArrayStream::empty());

    let stream_ptr = &*stream as *const ffi::ArrowArrayStream;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    ob.call_method1(py, "_export_to_c", (stream_ptr as Py_uintptr_t,))?;

    let mut iter =
        unsafe { ffi::ArrowArrayStreamReader::try_new(stream).map_err(PyO3ArrowError::from) }?;

    let mut arrays = vec![];
    while let Some(array) = unsafe { iter.next() } {
        let py_array = to_py_array(array.unwrap().into(), py)?;
        arrays.push(py_array)
    }
    Ok(arrays)
}
