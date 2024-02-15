use crate::buffer::Buffer;
use crate::error::{Error, Result};

#[derive(Debug)]
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

impl From<u128> for View {
    #[inline]
    fn from(value: u128) -> Self {
        Self {
            length: value as u32,
            prefix: (value >> 32) as u32,
            buffer_idx: (value >> 64) as u32,
            offset: (value >> 96) as u32,
        }
    }
}

impl From<View> for u128 {
    #[inline]
    fn from(value: View) -> Self {
        value.length as u128
            | ((value.prefix as u128) << 32)
            | ((value.buffer_idx as u128) << 64)
            | ((value.offset as u128) << 96)
    }
}

fn validate_view<F>(views: &[u128], buffers: &[Buffer<u8>], validate_bytes: F) -> Result<()>
where
    F: Fn(&[u8]) -> Result<()>,
{
    for view in views {
        let len = *view as u32;
        if len <= 12 {
            if len < 12 && view >> (32 + len * 8) != 0 {
                return Err(Error::InvalidArgumentError(format!(
                    "View contained non-zero padding for string of length {len}",
                )));
            }

            validate_bytes(&view.to_le_bytes()[4..4 + len as usize])?;
        } else {
            let view = View::from(*view);

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

pub(super) fn validate_binary_view(views: &[u128], buffers: &[Buffer<u8>]) -> Result<()> {
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

pub(super) fn validate_utf8_view(views: &[u128], buffers: &[Buffer<u8>]) -> Result<()> {
    validate_view(views, buffers, validate_utf8)
}

pub(super) fn validate_utf8_only(views: &[u128], buffers: &[Buffer<u8>]) -> Result<()> {
    for view in views {
        let len = *view as u32;
        if len <= 12 {
            validate_utf8(
                view.to_le_bytes()
                    .get_unchecked_release(4..4 + len as usize),
            )?;
        } else {
            let buffer_idx = (*view >> 64) as u32;
            let offset = (*view >> 96) as u32;
            let data = buffers.get_unchecked_release(buffer_idx as usize);

            let start = offset as usize;
            let end = start + len as usize;
            let b = &data.as_slice().get_unchecked_release(start..end);
            validate_utf8(b)?;
        };
    }

    Ok(())
}
