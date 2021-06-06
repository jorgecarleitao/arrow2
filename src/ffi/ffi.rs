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

use std::{
    ffi::CStr,
    ffi::CString,
    ptr::{self, NonNull},
    sync::Arc,
};

use crate::{
    array::Array,
    bitmap::{utils::bytes_for, Bitmap},
    buffer::{
        bytes::{Bytes, Deallocation},
        Buffer,
    },
    datatypes::{DataType, Field, TimeUnit},
    error::{ArrowError, Result},
    types::NativeType,
};

#[allow(dead_code)]
struct SchemaPrivateData {
    field: Field,
    children_ptr: Box<[*mut Ffi_ArrowSchema]>,
}

/// ABI-compatible struct for `ArrowSchema` from C Data Interface
/// See <https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions>
/// This was created by bindgen
#[repr(C)]
#[derive(Debug)]
pub struct Ffi_ArrowSchema {
    format: *const ::std::os::raw::c_char,
    name: *const ::std::os::raw::c_char,
    metadata: *const ::std::os::raw::c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut Ffi_ArrowSchema,
    dictionary: *mut Ffi_ArrowSchema,
    release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut Ffi_ArrowSchema)>,
    private_data: *mut ::std::os::raw::c_void,
}

// callback used to drop [Ffi_ArrowSchema] when it is exported.
unsafe extern "C" fn c_release_schema(schema: *mut Ffi_ArrowSchema) {
    if schema.is_null() {
        return;
    }
    let schema = &mut *schema;

    // take ownership back to release it.
    CString::from_raw(schema.format as *mut std::os::raw::c_char);
    CString::from_raw(schema.name as *mut std::os::raw::c_char);
    let private = Box::from_raw(schema.private_data as *mut SchemaPrivateData);
    for child in private.children_ptr.iter() {
        let _ = Box::from_raw(*child);
    }

    schema.release = None;
}

impl Ffi_ArrowSchema {
    /// create a new [`Ffi_ArrowSchema`]. This fails if the fields' [`DataType`] is not supported.
    fn try_new(field: Field) -> Result<Ffi_ArrowSchema> {
        let format = to_format(field.data_type())?;
        let name = field.name().clone();

        // allocate (and hold) the children
        let children_vec = match field.data_type() {
            DataType::List(field) => {
                vec![Box::new(Ffi_ArrowSchema::try_new(field.as_ref().clone())?)]
            }
            DataType::LargeList(field) => {
                vec![Box::new(Ffi_ArrowSchema::try_new(field.as_ref().clone())?)]
            }
            DataType::Struct(fields) => fields
                .iter()
                .map(|field| Ok(Box::new(Ffi_ArrowSchema::try_new(field.clone())?)))
                .collect::<Result<Vec<_>>>()?,
            _ => vec![],
        };
        // note: this cannot be done along with the above because the above is fallible and this op leaks.
        let children_ptr = children_vec
            .into_iter()
            .map(Box::into_raw)
            .collect::<Box<_>>();
        let n_children = children_ptr.len() as i64;

        let flags = field.is_nullable() as i64 * 2;

        let mut private = Box::new(SchemaPrivateData {
            field,
            children_ptr,
        });

        // <https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema>
        Ok(Ffi_ArrowSchema {
            format: CString::new(format).unwrap().into_raw(),
            name: CString::new(name).unwrap().into_raw(),
            metadata: std::ptr::null_mut(),
            flags,
            n_children,
            children: private.children_ptr.as_mut_ptr(),
            dictionary: std::ptr::null_mut(),
            release: Some(c_release_schema),
            private_data: Box::into_raw(private) as *mut ::std::os::raw::c_void,
        })
    }

    /// create an empty [Ffi_ArrowSchema]
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
        assert!(!self.format.is_null());
        // safe because the lifetime of `self.format` equals `self`
        unsafe { CStr::from_ptr(self.format) }
            .to_str()
            .expect("The external API has a non-utf8 as format")
    }

    /// returns the name of this schema.
    pub fn name(&self) -> &str {
        assert!(!self.name.is_null());
        // safe because the lifetime of `self.name` equals `self`
        unsafe { CStr::from_ptr(self.name) }.to_str().unwrap()
    }

    pub fn child(&self, index: usize) -> &Self {
        assert!(index < self.n_children as usize);
        assert!(!self.name.is_null());
        unsafe { self.children.add(index).as_ref().unwrap().as_ref().unwrap() }
    }

    pub fn nullable(&self) -> bool {
        (self.flags / 2) & 1 == 1
    }
}

impl Drop for Ffi_ArrowSchema {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

/// See https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
fn to_field(schema: &Ffi_ArrowSchema) -> Result<Field> {
    let data_type = match schema.format() {
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
        "+l" => {
            let child = schema.child(0);
            DataType::List(Box::new(to_field(child)?))
        }
        "+L" => {
            let child = schema.child(0);
            DataType::LargeList(Box::new(to_field(child)?))
        }
        "+s" => {
            let children = (0..schema.n_children as usize)
                .map(|x| to_field(schema.child(x)))
                .collect::<Result<Vec<_>>>()?;
            DataType::Struct(children)
        }
        other => {
            return Err(ArrowError::Ffi(format!(
                "The datatype \"{}\" is still not supported in Rust implementation",
                other
            )))
        }
    };
    Ok(Field::new(schema.name(), data_type, schema.nullable()))
}

/// the inverse of [to_datatype]
fn to_format(data_type: &DataType) -> Result<String> {
    Ok(match data_type {
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
        DataType::List(_) => "+l",
        DataType::LargeList(_) => "+L",
        DataType::Struct(_) => "+s",
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
#[derive(Debug, Clone)]
pub struct Ffi_ArrowArray {
    pub(crate) length: i64,
    pub(crate) null_count: i64,
    pub(crate) offset: i64,
    pub(crate) n_buffers: i64,
    pub(crate) n_children: i64,
    pub(crate) buffers: *mut *const ::std::os::raw::c_void,
    children: *mut *mut Ffi_ArrowArray,
    dictionary: *mut Ffi_ArrowArray,
    release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut Ffi_ArrowArray)>,
    // When exported, this MUST contain everything that is owned by this array.
    // for example, any buffer pointed to in `buffers` must be here, as well as the `buffers` pointer
    // itself.
    // In other words, everything in [Ffi_ArrowArray] must be owned by `private_data` and can assume
    // that they do not outlive `private_data`.
    private_data: *mut ::std::os::raw::c_void,
}

impl Drop for Ffi_ArrowArray {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

// callback used to drop [Ffi_ArrowArray] when it is exported
unsafe extern "C" fn c_release_array(array: *mut Ffi_ArrowArray) {
    if array.is_null() {
        return;
    }
    let array = &mut *array;

    // take ownership of `private_data`, therefore dropping it
    let private = Box::from_raw(array.private_data as *mut PrivateData);
    for child in private.children_ptr.iter() {
        let _ = Box::from_raw(*child);
    }

    array.release = None;
}

#[allow(dead_code)]
struct PrivateData {
    array: Arc<dyn Array>,
    buffers_ptr: Box<[*const std::os::raw::c_void]>,
    children_ptr: Box<[*mut Ffi_ArrowArray]>,
}

impl Ffi_ArrowArray {
    /// creates a new `Ffi_ArrowArray` from existing data.
    /// # Safety
    /// This method releases `buffers`. Consumers of this struct *must* call `release` before
    /// releasing this struct, or contents in `buffers` leak.
    fn new(array: Arc<dyn Array>) -> Self {
        let buffers = array.buffers();
        let buffers_ptr = buffers
            .iter()
            .map(|maybe_buffer| match maybe_buffer {
                // note that `raw_data` takes into account the buffer's offset
                Some(b) => b.as_ptr() as *const std::os::raw::c_void,
                None => std::ptr::null(),
            })
            .collect::<Box<[_]>>();
        let n_buffers = buffers.len() as i64;

        let children_ptr = array
            .children()
            .into_iter()
            .map(|child| Box::into_raw(Box::new(Ffi_ArrowArray::new(child))))
            .collect::<Box<_>>();
        let n_children = children_ptr.len() as i64;

        let length = array.len() as i64;
        let null_count = array.null_count() as i64;

        let mut private_data = Box::new(PrivateData {
            array,
            buffers_ptr,
            children_ptr,
        });

        Self {
            length,
            null_count,
            offset: 0i64,
            n_buffers,
            n_children,
            buffers: private_data.buffers_ptr.as_mut_ptr(),
            children: private_data.children_ptr.as_mut_ptr(),
            dictionary: std::ptr::null_mut(),
            release: Some(c_release_array),
            private_data: Box::into_raw(private_data) as *mut ::std::os::raw::c_void,
        }
    }

    // create an empty `Ffi_ArrowArray`, which can be used to import data into
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

    /// the length of the array
    pub fn len(&self) -> usize {
        self.length as usize
    }

    /// whether the array is empty
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// the offset of the array
    pub fn offset(&self) -> usize {
        self.offset as usize
    }

    /// the null count of the array
    pub fn null_count(&self) -> usize {
        self.null_count as usize
    }
}

/// interprets the buffer `index` as a [`Buffer`].
/// # Safety
/// The caller must guarantee that the buffer `index` corresponds to a buffer of type `T`.
/// This function assumes that the buffer created from FFI is valid; this is impossible to prove.
unsafe fn create_buffer<T: NativeType>(
    array: &Ffi_ArrowArray,
    data_type: &DataType,
    deallocation: Deallocation,
    index: usize,
) -> Result<Buffer<T>> {
    if array.buffers.is_null() {
        return Err(ArrowError::Ffi("The array buffers are null".to_string()));
    }
    let buffers = array.buffers as *mut *const u8;

    let len = buffer_len(array, data_type, index)?;

    assert!(index < array.n_buffers as usize);
    let ptr = *buffers.add(index);

    let ptr = NonNull::new(ptr as *mut T);
    let bytes = ptr
        .map(|ptr| Bytes::new(ptr, len, deallocation))
        .ok_or_else(|| ArrowError::Ffi(format!("The buffer at position {} is null", index)));

    bytes.map(Buffer::from_bytes)
}

/// returns a new buffer corresponding to the index `i` of the FFI array. It may not exist (null pointer).
/// `bits` is the number of bits that the native type of this buffer has.
/// The size of the buffer will be `ceil(self.length * bits, 8)`.
/// # Panic
/// This function panics if `i` is larger or equal to `n_buffers`.
/// # Safety
/// This function assumes that `ceil(self.length * bits, 8)` is the size of the buffer
unsafe fn create_bitmap(
    array: &Ffi_ArrowArray,
    deallocation: Deallocation,
    index: usize,
) -> Result<Bitmap> {
    if array.buffers.is_null() {
        return Err(ArrowError::Ffi("The array buffers are null".to_string()));
    }
    let len = array.length as usize;
    let buffers = array.buffers as *mut *const u8;

    assert!(index < array.n_buffers as usize);
    let ptr = *buffers.add(index);

    let bytes_len = bytes_for(len);
    let ptr = NonNull::new(ptr as *mut u8);
    let bytes = ptr
        .map(|ptr| Bytes::new(ptr, bytes_len, deallocation))
        .ok_or_else(|| {
            ArrowError::Ffi(format!(
                "The buffer {} is a null pointer and cannot be interpreted as a bitmap",
                index
            ))
        })?;

    Ok(Bitmap::from_bytes(bytes, len))
}

/// Returns the length, in slots, of the buffer `i` (indexed according to the C data interface)
// Rust implementation uses fixed-sized buffers, which require knowledge of their `len`.
// for variable-sized buffers, such as the second buffer of a stringArray, we need
// to fetch offset buffer's len to build the second buffer.
fn buffer_len(array: &Ffi_ArrowArray, data_type: &DataType, i: usize) -> Result<usize> {
    Ok(match (data_type, i) {
        (DataType::Utf8, 1)
        | (DataType::LargeUtf8, 1)
        | (DataType::Binary, 1)
        | (DataType::LargeBinary, 1)
        | (DataType::List(_), 1)
        | (DataType::LargeList(_), 1) => {
            // the len of the offset buffer (buffer 1) equals length + 1
            array.length as usize + 1
        }
        (DataType::Utf8, 2) | (DataType::Binary, 2) => {
            // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
            let len = buffer_len(array, data_type, 1)?;
            // first buffer is the null buffer => add(1)
            let offset_buffer = unsafe { *(array.buffers as *mut *const u8).add(1) };
            // interpret as i32
            let offset_buffer = offset_buffer as *const i32;
            // get last offset
            (unsafe { *offset_buffer.add(len - 1) }) as usize
        }
        (DataType::LargeUtf8, 2) | (DataType::LargeBinary, 2) => {
            // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
            let len = buffer_len(array, data_type, 1)?;
            // first buffer is the null buffer => add(1)
            let offset_buffer = unsafe { *(array.buffers as *mut *const u8).add(1) };
            // interpret as i64
            let offset_buffer = offset_buffer as *const i64;
            // get last offset
            (unsafe { *offset_buffer.add(len - 1) }) as usize
        }
        // buffer len of primitive types
        _ => array.length as usize,
    })
}

fn create_child(
    array: &Ffi_ArrowArray,
    schema: &Ffi_ArrowSchema,
    parent: Arc<ArrowArray>,
    index: usize,
) -> Result<ArrowArrayChild<'static>> {
    assert!(index < array.n_children as usize);
    assert!(!array.children.is_null());
    assert!(!array.children.is_null());
    unsafe {
        let arr_ptr = *array.children.add(index);
        let schema_ptr = *schema.children.add(index);
        assert!(!arr_ptr.is_null());
        assert!(!schema_ptr.is_null());
        let arr_ptr = &*arr_ptr;
        let schema_ptr = &*schema_ptr;
        Ok(ArrowArrayChild::from_raw(arr_ptr, schema_ptr, parent))
    }
}

pub trait ArrowArrayRef {
    fn deallocation(&self) -> Deallocation {
        Deallocation::Foreign(self.parent().clone())
    }

    /// returns the null bit buffer.
    /// Rust implementation uses a buffer that is not part of the array of buffers.
    /// The C Data interface's null buffer is part of the array of buffers.
    /// # Safety
    /// The caller must guarantee that the buffer `index` corresponds to a bitmap.
    /// This function assumes that the bitmap created from FFI is valid; this is impossible to prove.
    unsafe fn validity(&self) -> Result<Option<Bitmap>> {
        if self.array().null_count() == 0 {
            Ok(None)
        } else {
            create_bitmap(self.array(), self.deallocation(), 0).map(Some)
        }
    }

    /// # Safety
    /// The caller must guarantee that the buffer `index` corresponds to a bitmap.
    /// This function assumes that the bitmap created from FFI is valid; this is impossible to prove.
    unsafe fn buffer<T: NativeType>(&self, index: usize) -> Result<Buffer<T>> {
        // +1 to ignore null bitmap
        create_buffer::<T>(
            self.array(),
            &self.data_type()?,
            self.deallocation(),
            index + 1,
        )
    }

    /// # Safety
    /// The caller must guarantee that the buffer `index` corresponds to a bitmap.
    /// This function assumes that the bitmap created from FFI is valid; this is impossible to prove.
    unsafe fn bitmap(&self, index: usize) -> Result<Bitmap> {
        // +1 to ignore null bitmap
        create_bitmap(self.array(), self.deallocation(), index + 1)
    }

    fn child(&self, index: usize) -> Result<ArrowArrayChild> {
        create_child(self.array(), self.schema(), self.parent().clone(), index)
    }

    fn parent(&self) -> &Arc<ArrowArray>;
    fn array(&self) -> &Ffi_ArrowArray;
    fn schema(&self) -> &Ffi_ArrowSchema;
    fn data_type(&self) -> Result<DataType>;
}

/// Struct used to move an Array from and to the C Data Interface.
/// Its main responsibility is to expose functionality that requires
/// both [Ffi_ArrowArray] and [Ffi_ArrowSchema].
///
/// This struct has two main paths:
///
/// ## Import from the C Data Interface
/// * [ArrowArray::empty] to allocate memory to be filled by an external call
/// * [ArrowArray::try_from_raw] to consume two non-null allocated pointers
/// ## Export to the C Data Interface
/// * [ArrowArray::try_new] to create a new [ArrowArray] from Rust-specific information
/// * [ArrowArray::into_raw] to expose two pointers for [Ffi_ArrowArray] and [Ffi_ArrowSchema].
///
/// # Safety
/// Whoever creates this struct is responsible for releasing their resources. Specifically,
/// consumers *must* call [ArrowArray::into_raw] and take ownership of the individual pointers,
/// calling [Ffi_ArrowArray::release] and [Ffi_ArrowSchema::release] accordingly.
///
/// Furthermore, this struct assumes that the incoming data agrees with the C data interface.
#[derive(Debug)]
pub struct ArrowArray {
    array: Arc<Ffi_ArrowArray>,
    schema: Arc<Ffi_ArrowSchema>,
}

impl ArrowArrayRef for Arc<ArrowArray> {
    /// the data_type as declared in the schema
    fn data_type(&self) -> Result<DataType> {
        to_field(&self.schema).map(|x| x.data_type().clone())
    }

    fn parent(&self) -> &Arc<ArrowArray> {
        &self
    }

    fn array(&self) -> &Ffi_ArrowArray {
        self.array.as_ref()
    }

    fn schema(&self) -> &Ffi_ArrowSchema {
        self.schema.as_ref()
    }
}

#[derive(Debug)]
pub struct ArrowArrayChild<'a> {
    array: &'a Ffi_ArrowArray,
    schema: &'a Ffi_ArrowSchema,
    parent: Arc<ArrowArray>,
}

impl<'a> ArrowArrayRef for ArrowArrayChild<'a> {
    /// the data_type as declared in the schema
    fn data_type(&self) -> Result<DataType> {
        to_field(self.schema).map(|x| x.data_type().clone())
    }

    fn parent(&self) -> &Arc<ArrowArray> {
        &self.parent
    }

    fn array(&self) -> &Ffi_ArrowArray {
        self.array
    }

    fn schema(&self) -> &Ffi_ArrowSchema {
        self.schema
    }

    fn child(&self, _: usize) -> Result<ArrowArrayChild> {
        todo!()
    }
}

impl<'a> ArrowArrayChild<'a> {
    fn from_raw(
        array: &'a Ffi_ArrowArray,
        schema: &'a Ffi_ArrowSchema,
        parent: Arc<ArrowArray>,
    ) -> Self {
        Self {
            array,
            schema,
            parent,
        }
    }
}

/// Exports an `Array` to the C data interface.
pub fn export_to_c(array: Arc<dyn Array>) -> Result<ArrowArray> {
    let field = Field::new("", array.data_type().clone(), array.null_count() != 0);

    Ok(ArrowArray {
        array: Arc::new(Ffi_ArrowArray::new(array)),
        schema: Arc::new(Ffi_ArrowSchema::try_new(field)?),
    })
}

pub fn create_empty() -> ArrowArray {
    ArrowArray {
        array: Arc::new(Ffi_ArrowArray::empty()),
        schema: Arc::new(Ffi_ArrowSchema::empty()),
    }
}

impl ArrowArray {
    pub fn references(&self) -> (*mut Ffi_ArrowArray, *mut Ffi_ArrowSchema) {
        (
            self.array.as_ref() as *const Ffi_ArrowArray as *mut Ffi_ArrowArray,
            self.schema.as_ref() as *const Ffi_ArrowSchema as *mut Ffi_ArrowSchema,
        )
    }
}
