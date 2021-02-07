use std::sync::Arc;

use crate::{
    bits::{get_bit, null_count, set_bit_raw, unset_bit_raw},
    buffer::bytes::Bytes,
    ffi,
};

use super::{bytes::Deallocation, MutableBuffer};

#[derive(Debug, Clone)]
pub struct Bitmap {
    bytes: Arc<Bytes<u8>>,
    // both are measured in bits. They are used to bound the bitmap to a region of Bytes.
    offset: usize,
    length: usize,
    // this is a cache: it must be computed on initialization
    null_count: usize,
}

impl Bitmap {
    pub fn new() -> Self {
        MutableBitmap::new().into()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn from_bytes(bytes: Bytes<u8>, length: usize) -> Self {
        assert!(length <= bytes.len() * 8);
        let null_count = null_count(&bytes, 0, length);
        Self {
            length,
            offset: 0,
            bytes: Arc::new(bytes),
            null_count,
        }
    }

    #[inline]
    pub fn null_count_range(&self, offset: usize, length: usize) -> usize {
        null_count(&self.bytes, self.offset + offset, length)
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.null_count
    }

    #[inline]
    pub fn slice(mut self, offset: usize, length: usize) -> Self {
        let offset = self.offset + offset;
        self.offset += offset;
        self.length = length;
        self.null_count = null_count(&self.bytes, self.offset, self.length);
        self
    }

    #[inline]
    pub fn get_bit(&self, i: usize) -> bool {
        get_bit(&self.bytes, self.offset + i)
    }

    /// Returns a pointer to the start of this bitmap.
    pub fn as_ptr(&self) -> std::ptr::NonNull<u8> {
        self.bytes.ptr()
    }
}

#[derive(Debug)]
pub struct MutableBitmap {
    buffer: MutableBuffer<u8>,
    length: usize,
}

impl MutableBitmap {
    #[inline]
    pub fn new() -> Self {
        Self {
            buffer: MutableBuffer::new(),
            length: 0,
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: MutableBuffer::from_len_zeroed(capacity.saturating_add(7) / 8),
            length: 0,
        }
    }

    #[inline]
    pub fn push(&mut self, value: bool) {
        self.buffer
            .resize((self.length + 1).saturating_add(7) / 8, 0);
        if value {
            unsafe { set_bit_raw(self.buffer.as_mut_ptr(), self.length) };
        } else {
            unsafe { unset_bit_raw(self.buffer.as_mut_ptr(), self.length) };
        }
        self.length += 1;
    }

    #[inline]
    pub unsafe fn push_unchecked(&mut self, value: bool) {
        if value {
            set_bit_raw(self.buffer.as_mut_ptr(), self.length);
        } else {
            unset_bit_raw(self.buffer.as_mut_ptr(), self.length);
        }
        self.length += 1;
        self.buffer.set_len(self.length.saturating_add(7) / 8);
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        null_count(&self.buffer, 0, self.length)
    }

    /// Returns the number of bytes in the buffer
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Safety
    /// The caller must ensure that the buffer was properly initialized up to `len`.
    #[inline]
    pub(crate) unsafe fn set_len(&mut self, len: usize) {
        self.buffer.set_len(len.saturating_add(7) / 8);
        self.length = len;
    }
}

impl From<MutableBitmap> for Bitmap {
    #[inline]
    fn from(buffer: MutableBitmap) -> Self {
        Bitmap::from_bytes(buffer.buffer.into(), buffer.length)
    }
}

impl Bitmap {
    /// Creates a bitmap from an existing memory region (must already be byte-aligned), this
    /// `Bitmap` **does not** free this piece of memory when dropped.
    ///
    /// # Arguments
    ///
    /// * `ptr` - Pointer to raw parts
    /// * `len` - Length of raw parts in **bytes**
    /// * `data` - An [ffi::FFI_ArrowArray] with the data
    ///
    /// # Safety
    ///
    /// This function is unsafe as there is no guarantee that the given pointer is valid for `len`
    /// bytes and that the foreign deallocator frees the region.
    pub unsafe fn from_unowned(
        ptr: std::ptr::NonNull<u8>,
        length: usize,
        data: Arc<ffi::FFI_ArrowArray>,
    ) -> Self {
        // todo: make all kinds of assertions
        let bytes = Bytes::new(ptr, length, Deallocation::Foreign(data));
        let null_count = null_count(&bytes, 0, length);
        Self {
            bytes: Arc::new(bytes),
            offset: 0,
            length,
            null_count,
        }
    }
}
