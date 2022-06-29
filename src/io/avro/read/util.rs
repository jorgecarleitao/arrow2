use std::collections::HashMap;
use std::io::Read;

use avro_schema::Schema;

use crate::error::{Error, Result};

use super::super::{avro_decode, read_header, read_metadata};
use super::{deserialize_header, Compression};

pub fn zigzag_i64<R: Read>(reader: &mut R) -> Result<i64> {
    let z = decode_variable(reader)?;
    Ok(if z & 0x1 == 0 {
        (z >> 1) as i64
    } else {
        !(z >> 1) as i64
    })
}

fn decode_variable<R: Read>(reader: &mut R) -> Result<u64> {
    avro_decode!(reader)
}

fn _read_binary<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let len: usize = zigzag_i64(reader)? as usize;
    let mut buf = vec![];
    buf.try_reserve(len)?;
    reader.take(len as u64).read_to_end(&mut buf)?;
    Ok(buf)
}

fn read_header<R: Read>(reader: &mut R) -> Result<HashMap<String, Vec<u8>>> {
    read_header!(reader)
}

fn read_file_marker<R: Read>(reader: &mut R) -> Result<[u8; 16]> {
    let mut marker = [0u8; 16];
    reader.read_exact(&mut marker)?;
    Ok(marker)
}

/// Reads the schema from `reader`, returning the file's [`Schema`] and [`Compression`].
/// # Error
/// This function errors iff the header is not a valid avro file header.
pub fn read_schema<R: Read>(reader: &mut R) -> Result<(Schema, Option<Compression>, [u8; 16])> {
    read_metadata!(reader)
}
