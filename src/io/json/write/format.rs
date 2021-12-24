use std::{fmt::Debug, io::Write};

use crate::error::Result;

/// Trait defining how to format a sequence of JSON objects to a byte stream.
pub trait JsonFormat: Debug + Default + Copy {
    #[inline]
    /// write any bytes needed at the start of the file to the writer
    fn start_stream<W: Write>(&self, _writer: &mut W) -> Result<()> {
        Ok(())
    }

    #[inline]
    /// write any bytes needed for the start of each row
    fn start_row<W: Write>(&self, _writer: &mut W, _is_first_row: bool) -> Result<()> {
        Ok(())
    }

    #[inline]
    /// write any bytes needed for the end of each row
    fn end_row<W: Write>(&self, _writer: &mut W) -> Result<()> {
        Ok(())
    }

    /// write any bytes needed for the start of each row
    fn end_stream<W: Write>(&self, _writer: &mut W) -> Result<()> {
        Ok(())
    }
}

/// Produces JSON output with one record per line. For example
///
/// ```json
/// {"foo":1}
/// {"bar":1}
///
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct LineDelimited {}

impl JsonFormat for LineDelimited {
    #[inline]
    fn end_row<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(b"\n")?;
        Ok(())
    }
}

/// Produces JSON output as a single JSON array. For example
///
/// ```json
/// [{"foo":1},{"bar":1}]
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonArray {}

impl JsonFormat for JsonArray {
    #[inline]
    fn start_stream<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(b"[")?;
        Ok(())
    }

    #[inline]
    fn start_row<W: Write>(&self, writer: &mut W, is_first_row: bool) -> Result<()> {
        if !is_first_row {
            writer.write_all(b",")?;
        }
        Ok(())
    }

    #[inline]
    fn end_stream<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(b"]")?;
        Ok(())
    }
}
