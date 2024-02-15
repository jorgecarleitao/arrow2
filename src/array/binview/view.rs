use crate::buffer::Buffer;
use crate::error::{Error, Result};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::ops::Add;

use bytemuck::{Pod, Zeroable};

use crate::datatypes::PrimitiveType;
use crate::types::NativeType;

// We use this instead of u128 because we want alignment of <= 8 bytes.
#[derive(Debug, Copy, Clone, Default)]
#[repr(C)]
pub struct View {
    /// The length of the string/bytes.
    pub length: u32,
    /// First 4 bytes of string/bytes data.
    pub prefix: u32,
    /// The buffer index.
    pub buffer_idx: u32,
    /// The offset into the buffer.
    pub offset: u32,
}

impl View {
    #[inline(always)]
    pub fn as_u128(self) -> u128 {
        unsafe { std::mem::transmute(self) }
    }
}

impl Display for View {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

unsafe impl Zeroable for View {}

unsafe impl Pod for View {}

impl Add<Self> for View {
    type Output = View;

    fn add(self, _rhs: Self) -> Self::Output {
        unimplemented!()
    }
}

impl num_traits::Zero for View {
    fn zero() -> Self {
        Default::default()
    }

    fn is_zero(&self) -> bool {
        *self == Self::zero()
    }
}

impl PartialEq for View {
    fn eq(&self, other: &Self) -> bool {
        self.as_u128() == other.as_u128()
    }
}

impl NativeType for View {
    const PRIMITIVE: PrimitiveType = PrimitiveType::UInt128;
    type Bytes = [u8; 16];

    #[inline]
    fn to_le_bytes(&self) -> Self::Bytes {
        self.as_u128().to_le_bytes()
    }

    #[inline]
    fn to_be_bytes(&self) -> Self::Bytes {
        self.as_u128().to_be_bytes()
    }

    #[inline]
    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        Self::from(u128::from_le_bytes(bytes))
    }

    #[inline]
    fn from_be_bytes(bytes: Self::Bytes) -> Self {
        Self::from(u128::from_be_bytes(bytes))
    }
}

impl From<u128> for View {
    #[inline]
    fn from(value: u128) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl From<View> for u128 {
    #[inline]
    fn from(value: View) -> Self {
        value.as_u128()
    }
}

fn validate_view<F>(views: &[View], buffers: &[Buffer<u8>], validate_bytes: F) -> Result<()>
where
    F: Fn(&[u8]) -> Result<()>,
{
    for view in views {
        let len = view.length;
        if len <= 12 {
            if len < 12 && view.as_u128() >> (32 + len * 8) != 0 {
                return Err(Error::InvalidArgumentError(format!(
                    "View contained non-zero padding for string of length {len}",
                )));
            }

            validate_bytes(&view.to_le_bytes()[4..4 + len as usize])?;
        } else {
            let data = buffers.get(view.buffer_idx as usize).ok_or_else(|| {
                Error::InvalidArgumentError(format!(
                    "Invalid buffer index: got index {} but only has {} buffers",
                    view.buffer_idx,
                    buffers.len()
                ))
            })?;

            let start = view.offset as usize;
            let end = start + len as usize;
            let b = data
                .as_slice()
                .get(start..end)
                .ok_or_else(|| {
                    Error::InvalidArgumentError(format!(
                        "Invalid buffer slice: got {start}..{end} but buffer {} has length {}",
                        view.buffer_idx,
                        data.len()
                    ))
                })?;

            if !b.starts_with(&view.prefix.to_le_bytes()) {
                return Err(Error::InvalidArgumentError(
                    "Mismatch between embedded prefix and data".to_string(),
                ));
            }
            validate_bytes(b)?;
        };
    }

    Ok(())
}

pub(super) fn validate_binary_view(views: &[View], buffers: &[Buffer<u8>]) -> Result<()> {
    validate_view(views, buffers, |_| Ok(()))
}

fn validate_utf8(b: &[u8]) -> Result<()> {
    match simdutf8::basic::from_utf8(b) {
        Ok(_) => Ok(()),
        Err(e) => Err(Error::InvalidArgumentError(format!(
            "Encountered non-UTF-8 data {e}"
        )))
    }
}

pub(super) fn validate_utf8_view(views: &[View], buffers: &[Buffer<u8>]) -> Result<()> {
    validate_view(views, buffers, validate_utf8)
}

/// # Safety
/// The views and buffers must uphold the invariants of BinaryView otherwise we will go OOB.
pub(super) unsafe fn validate_utf8_only(
    views: &[View],
    buffers: &[Buffer<u8>],
) -> Result<()> {
    for view in views {
        let len = view.length;
        if len <= 12 {
            validate_utf8(
                view.to_le_bytes()
                    .get_unchecked_release(4..4 + len as usize),
            )?;
        } else {
            let buffer_idx = view.buffer_idx;
            let offset = view.offset;
            let data = buffers.get_unchecked_release(buffer_idx as usize);

            let start = offset as usize;
            let end = start + len as usize;
            let b = &data.as_slice().get_unchecked_release(start..end);
            validate_utf8(b)?;
        };
    }

    Ok(())
}
