// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/*
# Design:

Main assumptions:
* A memory region is deallocated according it its own release mechanism.
* Rust shares memory regions between arrays.
* A memory region should be deallocated when no-one is using it.

The design of this module is as follows:

`ArrowArray` contains two `Arc`s, one per ABI-compatible `struct`, each containing data
according to the C Data Interface. These Arcs are used for ref counting of the structs
within Rust and lifetime management.

Each ABI-compatible `struct` knowns how to `drop` itself, calling `release`.

To import an array, unsafely create an `ArrowArray` from two pointers using [ArrowArray::try_from_raw].
To export an array, create an `ArrowArray` using [ArrowArray::try_new].
*/

use std::{
    ffi::CStr,
    ffi::CString,
    ptr::{self, NonNull},
    sync::Arc,
};

use crate::{
    array::Array,
    bitmap::Bitmap,
    bits::bytes_for,
    buffer::{
        bytes::{Bytes, Deallocation},
        Buffer,
    },
    datatypes::{DataType, TimeUnit},
    error::{ArrowError, Result},
    types::NativeType,
};

/// ABI-compatible struct for `ArrowSchema` from C Data Interface
/// See <https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions>
/// This was created by bindgen
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ArrowSchema {
    format: *const ::std::os::raw::c_char,
    name: *const ::std::os::raw::c_char,
    metadata: *const ::std::os::raw::c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut FFI_ArrowSchema,
    dictionary: *mut FFI_ArrowSchema,
    release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowSchema)>,
    private_data: *mut ::std::os::raw::c_void,
}

// callback used to drop [FFI_ArrowSchema] when it is exported.
unsafe extern "C" fn release_schema(schema: *mut FFI_ArrowSchema) {
    let schema = &mut *schema;

    // take ownership back to release it.
    CString::from_raw(schema.format as *mut std::os::raw::c_char);

    schema.release = None;
}

impl FFI_ArrowSchema {
    /// create a new [FFI_ArrowSchema] from a format.
    fn new(format: &str) -> FFI_ArrowSchema {
        // <https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema>
        FFI_ArrowSchema {
            format: CString::new(format).unwrap().into_raw(),
            name: std::ptr::null_mut(),
            metadata: std::ptr::null_mut(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: Some(release_schema),
            private_data: std::ptr::null_mut(),
        }
    }

    /// create an empty [FFI_ArrowSchema]
    fn empty() -> Self {
        Self {
            format: std::ptr::null_mut(),
            name: std::ptr::null_mut(),
            metadata: std::ptr::null_mut(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    /// returns the format of this schema.
    pub fn format(&self) -> &str {
        unsafe { CStr::from_ptr(self.format) }
            .to_str()
            .expect("The external API has a non-utf8 as format")
    }
}

impl Drop for FFI_ArrowSchema {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

/// maps a DataType `format` to a [DataType](arrow::datatypes::DataType).
/// See https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
fn to_datatype(format: &str) -> Result<DataType> {
    Ok(match format {
        "n" => DataType::Null,
        "b" => DataType::Boolean,
        "c" => DataType::Int8,
        "C" => DataType::UInt8,
        "s" => DataType::Int16,
        "S" => DataType::UInt16,
        "i" => DataType::Int32,
        "I" => DataType::UInt32,
        "l" => DataType::Int64,
        "L" => DataType::UInt64,
        "e" => DataType::Float16,
        "f" => DataType::Float32,
        "g" => DataType::Float64,
        "z" => DataType::Binary,
        "Z" => DataType::LargeBinary,
        "u" => DataType::Utf8,
        "U" => DataType::LargeUtf8,
        "tdD" => DataType::Date32,
        "tdm" => DataType::Date64,
        "tts" => DataType::Time32(TimeUnit::Second),
        "ttm" => DataType::Time32(TimeUnit::Millisecond),
        "ttu" => DataType::Time64(TimeUnit::Microsecond),
        "ttn" => DataType::Time64(TimeUnit::Nanosecond),
        _ => {
            return Err(ArrowError::Ffi(
                "The datatype \"{}\" is still not supported in Rust implementation".to_string(),
            ))
        }
    })
}

/// the inverse of [to_datatype]
fn from_datatype(datatype: &DataType) -> Result<String> {
    Ok(match datatype {
        DataType::Null => "n",
        DataType::Boolean => "b",
        DataType::Int8 => "c",
        DataType::UInt8 => "C",
        DataType::Int16 => "s",
        DataType::UInt16 => "S",
        DataType::Int32 => "i",
        DataType::UInt32 => "I",
        DataType::Int64 => "l",
        DataType::UInt64 => "L",
        DataType::Float16 => "e",
        DataType::Float32 => "f",
        DataType::Float64 => "g",
        DataType::Binary => "z",
        DataType::LargeBinary => "Z",
        DataType::Utf8 => "u",
        DataType::LargeUtf8 => "U",
        DataType::Date32 => "tdD",
        DataType::Date64 => "tdm",
        DataType::Time32(TimeUnit::Second) => "tts",
        DataType::Time32(TimeUnit::Millisecond) => "ttm",
        DataType::Time64(TimeUnit::Microsecond) => "ttu",
        DataType::Time64(TimeUnit::Nanosecond) => "ttn",
        z => {
            return Err(ArrowError::Ffi(format!(
                "The datatype \"{:?}\" is still not supported in Rust implementation",
                z
            )))
        }
    }
    .to_string())
}

/// ABI-compatible struct for ArrowArray from C Data Interface
/// See <https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions>
/// This was created by bindgen
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ArrowArray {
    pub(crate) length: i64,
    pub(crate) null_count: i64,
    pub(crate) offset: i64,
    pub(crate) n_buffers: i64,
    pub(crate) n_children: i64,
    pub(crate) buffers: *mut *const ::std::os::raw::c_void,
    children: *mut *mut FFI_ArrowArray,
    dictionary: *mut FFI_ArrowArray,
    release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArray)>,
    // When exported, this MUST contain everything that is owned by this array.
    // for example, any buffer pointed to in `buffers` must be here, as well as the `buffers` pointer
    // itself.
    // In other words, everything in [FFI_ArrowArray] must be owned by `private_data` and can assume
    // that they do not outlive `private_data`.
    private_data: *mut ::std::os::raw::c_void,
}

// callback used to drop [FFI_ArrowArray] when it is exported
unsafe extern "C" fn release_array(array: *mut FFI_ArrowArray) {
    if array.is_null() {
        return;
    }
    let array = &mut *array;
    // take ownership of `private_data`, therefore dropping it
    Box::from_raw(array.private_data as *mut PrivateData);

    array.release = None;
}

#[allow(dead_code)]
struct PrivateData {
    array: Arc<dyn Array>,
    buffers_ptr: Box<[*const std::os::raw::c_void]>,
}

impl FFI_ArrowArray {
    /// creates a new `FFI_ArrowArray` from existing data.
    /// # Safety
    /// This method releases `buffers`. Consumers of this struct *must* call `release` before
    /// releasing this struct, or contents in `buffers` leak.
    fn new(array: Arc<dyn Array>) -> Self {
        let buffers_ptr = array
            .buffers()
            .iter()
            .map(|maybe_buffer| match maybe_buffer {
                // note that `raw_data` takes into account the buffer's offset
                Some(b) => b.as_ptr() as *const std::os::raw::c_void,
                None => std::ptr::null(),
            })
            .collect::<Box<[_]>>();
        let pointer = buffers_ptr.as_ptr() as *mut *const std::ffi::c_void;

        Self {
            length: array.len() as i64,
            null_count: array.null_count() as i64,
            offset: 0i64,
            n_buffers: 3, // todo: fix me
            n_children: 0,
            buffers: pointer,
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: Some(release_array),
            private_data: Box::into_raw(Box::new(PrivateData { array, buffers_ptr }))
                as *mut ::std::os::raw::c_void,
        }
    }

    // create an empty `FFI_ArrowArray`, which can be used to import data into
    fn empty() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: std::ptr::null_mut(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }
}

/// returns a new buffer corresponding to the index `i` of the FFI array. It may not exist (null pointer).
/// `bits` is the number of bits that the native type of this buffer has.
/// # Panic
/// This function panics if `i` is larger or equal to `n_buffers`.
/// # Safety
/// This function assumes that `ceil(self.length * bits, 8)` is the size of the buffer
unsafe fn create_buffer<T: NativeType>(
    array: Arc<FFI_ArrowArray>,
    index: usize,
    len: usize,
) -> Option<Buffer<T>> {
    if array.buffers.is_null() {
        return None;
    }
    let buffers = array.buffers as *mut *const u8;

    assert!(index < array.n_buffers as usize);
    let ptr = *buffers.add(index);

    let ptr = NonNull::new(ptr as *mut T);
    let bytes = ptr.map(|ptr| Bytes::new(ptr, len, Deallocation::Foreign(array)));

    bytes.map(Buffer::from_bytes)
}

/// returns a new buffer corresponding to the index `i` of the FFI array. It may not exist (null pointer).
/// `bits` is the number of bits that the native type of this buffer has.
/// The size of the buffer will be `ceil(self.length * bits, 8)`.
/// # Panic
/// This function panics if `i` is larger or equal to `n_buffers`.
/// # Safety
/// This function assumes that `ceil(self.length * bits, 8)` is the size of the buffer
unsafe fn create_bitmap(array: Arc<FFI_ArrowArray>, index: usize, len: usize) -> Option<Bitmap> {
    if array.buffers.is_null() {
        return None;
    }
    let buffers = array.buffers as *mut *const u8;

    assert!(index < array.n_buffers as usize);
    let ptr = *buffers.add(index);

    let bytes_len = len.saturating_add(7) / 8;
    let ptr = NonNull::new(ptr as *mut u8);
    let bytes = ptr.map(|ptr| Bytes::new(ptr, bytes_len, Deallocation::Foreign(array)));

    bytes.map(|bytes| Bitmap::from_bytes(bytes, len))
}

impl Drop for FFI_ArrowArray {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

/// Struct used to move an Array from and to the C Data Interface.
/// Its main responsibility is to expose functionality that requires
/// both [FFI_ArrowArray] and [FFI_ArrowSchema].
///
/// This struct has two main paths:
///
/// ## Import from the C Data Interface
/// * [ArrowArray::empty] to allocate memory to be filled by an external call
/// * [ArrowArray::try_from_raw] to consume two non-null allocated pointers
/// ## Export to the C Data Interface
/// * [ArrowArray::try_new] to create a new [ArrowArray] from Rust-specific information
/// * [ArrowArray::into_raw] to expose two pointers for [FFI_ArrowArray] and [FFI_ArrowSchema].
///
/// # Safety
/// Whoever creates this struct is responsible for releasing their resources. Specifically,
/// consumers *must* call [ArrowArray::into_raw] and take ownership of the individual pointers,
/// calling [FFI_ArrowArray::release] and [FFI_ArrowSchema::release] accordingly.
///
/// Furthermore, this struct assumes that the incoming data agrees with the C data interface.
#[derive(Debug)]
pub struct ArrowArray {
    // these are ref-counted because they can be shared by multiple buffers.
    array: Arc<FFI_ArrowArray>,
    schema: Arc<FFI_ArrowSchema>,
}

impl ArrowArray {
    /// creates a new `ArrowArray`. This is used to export to the C Data Interface.
    /// # Safety
    /// See safety of [ArrowArray]
    pub fn try_new(array: Arc<dyn Array>) -> Result<Self> {
        let format = from_datatype(array.data_type())?;

        let schema = Arc::new(FFI_ArrowSchema::new(&format));
        let array = Arc::new(FFI_ArrowArray::new(array));

        Ok(ArrowArray { schema, array })
    }

    /// creates a new [ArrowArray] from two pointers. Used to import from the C Data Interface.
    /// # Safety
    /// See safety of [ArrowArray]
    /// # Error
    /// Errors if any of the pointers is null
    pub unsafe fn try_from_raw(
        array: *const FFI_ArrowArray,
        schema: *const FFI_ArrowSchema,
    ) -> Result<Self> {
        if array.is_null() || schema.is_null() {
            return Err(ArrowError::Ffi(
                "At least one of the pointers passed to `try_from_raw` is null".to_string(),
            ));
        };
        Ok(Self {
            array: Arc::from_raw(array as *mut FFI_ArrowArray),
            schema: Arc::from_raw(schema as *mut FFI_ArrowSchema),
        })
    }

    /// creates a new empty [ArrowArray]. Used to import from the C Data Interface.
    /// # Safety
    /// See safety of [ArrowArray]
    pub unsafe fn empty() -> Self {
        let schema = Arc::new(FFI_ArrowSchema::empty());
        let array = Arc::new(FFI_ArrowArray::empty());
        ArrowArray { schema, array }
    }

    /// exports [ArrowArray] to the C Data Interface
    pub fn into_raw(this: ArrowArray) -> (*const FFI_ArrowArray, *const FFI_ArrowSchema) {
        (Arc::into_raw(this.array), Arc::into_raw(this.schema))
    }

    /// returns the null bit buffer.
    /// Rust implementation uses a buffer that is not part of the array of buffers.
    /// The C Data interface's null buffer is part of the array of buffers.
    pub fn validity(&self) -> Option<Bitmap> {
        let len = self.array.length;
        unsafe { create_bitmap(self.array.clone(), 0, len as usize) }
    }

    /// Returns the length, in slots, of the buffer `i` (indexed according to the C data interface)
    // Rust implementation uses fixed-sized buffers, which require knowledge of their `len`.
    // for variable-sized buffers, such as the second buffer of a stringArray, we need
    // to fetch offset buffer's len to build the second buffer.
    fn buffer_len(&self, i: usize) -> Result<usize> {
        let data_type = &self.data_type()?;

        Ok(match (data_type, i) {
            (DataType::Utf8, 1)
            | (DataType::LargeUtf8, 1)
            | (DataType::Binary, 1)
            | (DataType::LargeBinary, 1) => {
                // the len of the offset buffer (buffer 1) equals length + 1
                self.array.length as usize + 1
            }
            (DataType::Utf8, 2) | (DataType::Binary, 2) => {
                // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
                let len = self.buffer_len(1)?;
                // first buffer is the null buffer => add(1)
                let offset_buffer = unsafe { *(self.array.buffers as *mut *const u8).add(1) };
                // interpret as i32
                let offset_buffer = offset_buffer as *const i32;
                // get last offset
                (unsafe { *offset_buffer.add(len - 1) }) as usize
            }
            (DataType::LargeUtf8, 2) | (DataType::LargeBinary, 2) => {
                // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
                let len = self.buffer_len(1)?;
                // first buffer is the null buffer => add(1)
                let offset_buffer = unsafe { *(self.array.buffers as *mut *const u8).add(1) };
                // interpret as i64
                let offset_buffer = offset_buffer as *const i64;
                // get last offset
                (unsafe { *offset_buffer.add(len - 1) }) as usize
            }
            // buffer len of primitive types
            _ => self.array.length as usize,
        })
    }

    /// returns all buffers, as organized by Rust (i.e. null buffer is skipped)
    /// # Safety
    /// The caller must guarantee that the buffer `index` corresponds to a buffer of type `T`.
    /// This function assumes that the buffer created from FFI is valid; this is impossible to prove.
    pub unsafe fn buffer<T: NativeType>(&self, index: usize) -> Result<Buffer<T>> {
        // + 1: skip null buffer
        let index = (index + 1) as usize;

        let len = self.buffer_len(index)?;

        create_buffer(self.array.clone(), index, len).ok_or_else(|| {
            ArrowError::Ffi(format!(
                "The external buffer at position {} is null.",
                index - 1
            ))
        })
    }

    /// returns all buffers, as organized by Rust (i.e. null buffer is skipped)
    /// # Safety
    /// The caller must guarantee that the buffer `index` corresponds to a bitmap.
    /// This function assumes that the buffer created from FFI is valid; this is impossible to prove.
    pub unsafe fn bitmap(&self, index: usize) -> Result<Bitmap> {
        // + 1: skip null buffer
        let index = (index + 1) as usize;

        let len = bytes_for(self.array.length as usize);

        create_bitmap(self.array.clone(), index, len).ok_or_else(|| {
            ArrowError::Ffi(format!(
                "The external buffer at position {} is null.",
                index - 1
            ))
        })
    }

    /// the length of the array
    pub fn len(&self) -> usize {
        self.array.length as usize
    }

    /// whether the array is empty
    pub fn is_empty(&self) -> bool {
        self.array.length == 0
    }

    /// the offset of the array
    pub fn offset(&self) -> usize {
        self.array.offset as usize
    }

    /// the null count of the array
    pub fn null_count(&self) -> usize {
        self.array.null_count as usize
    }

    /// the data_type as declared in the schema
    pub fn data_type(&self) -> Result<DataType> {
        to_datatype(self.schema.format())
    }
}
