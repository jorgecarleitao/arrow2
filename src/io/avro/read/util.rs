use std::collections::HashMap;
use std::io::Read;

use avro_rs::Schema;

use crate::error::{ArrowError, Result};

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
    let mut i = 0u64;
    let mut buf = [0u8; 1];

    let mut j = 0;
    loop {
        if j > 9 {
            // if j * 7 > 64
            panic!()
        }
        reader.read_exact(&mut buf[..])?;
        i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
        if (buf[0] >> 7) == 0 {
            break;
        } else {
            j += 1;
        }
    }

    Ok(i)
}

fn _read_binary<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let len: usize = zigzag_i64(reader)? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_header<R: Read>(reader: &mut R) -> Result<HashMap<String, Vec<u8>>> {
    let mut items = HashMap::new();

    loop {
        let len = zigzag_i64(reader)? as usize;
        if len == 0 {
            break Ok(items);
        }

        items.reserve(len);
        for _ in 0..len {
            let key = _read_binary(reader)?;
            let key = String::from_utf8(key)
                .map_err(|_| ArrowError::ExternalFormat("Invalid Avro header".to_string()))?;
            let value = _read_binary(reader)?;
            items.insert(key, value);
        }
    }
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
    let mut magic_number = [0u8; 4];
    reader.read_exact(&mut magic_number)?;

    // see https://avro.apache.org/docs/current/spec.html#Object+Container+Files
    if magic_number != [b'O', b'b', b'j', 1u8] {
        return Err(ArrowError::ExternalFormat(
            "Avro header does not contain a valid magic number".to_string(),
        ));
    }

    let header = read_header(reader)?;

    let (schema, compression) = deserialize_header(header)?;

    let marker = read_file_marker(reader)?;

    Ok((schema, compression, marker))
}
