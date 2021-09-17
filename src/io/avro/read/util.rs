use std::io::Read;
use std::str::FromStr;

use crate::error::Result;

use avro_rs::{from_avro_datum, types::Value, AvroResult, Codec, Error, Schema};
use serde_json::from_slice;

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

fn read_file_marker<R: std::io::Read>(reader: &mut R) -> AvroResult<[u8; 16]> {
    let mut marker = [0u8; 16];
    reader.read_exact(&mut marker).map_err(Error::ReadMarker)?;
    Ok(marker)
}

/// Reads the schema from `reader`, returning the file's [`Schema`] and [`Codec`].
/// # Error
/// This function errors iff the header is not a valid avro file header.
pub fn read_schema<R: Read>(reader: &mut R) -> AvroResult<(Schema, Codec, [u8; 16])> {
    let meta_schema = Schema::Map(Box::new(Schema::Bytes));

    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).map_err(Error::ReadHeader)?;

    if buf != [b'O', b'b', b'j', 1u8] {
        return Err(Error::HeaderMagic);
    }

    if let Value::Map(meta) = from_avro_datum(&meta_schema, reader, None)? {
        // TODO: surface original parse schema errors instead of coalescing them here
        let json = meta
            .get("avro.schema")
            .and_then(|bytes| {
                if let Value::Bytes(ref bytes) = *bytes {
                    from_slice(bytes.as_ref()).ok()
                } else {
                    None
                }
            })
            .ok_or(Error::GetAvroSchemaFromMap)?;
        let schema = Schema::parse(&json)?;

        let codec = if let Some(codec) = meta
            .get("avro.codec")
            .and_then(|codec| {
                if let Value::Bytes(ref bytes) = *codec {
                    std::str::from_utf8(bytes.as_ref()).ok()
                } else {
                    None
                }
            })
            .and_then(|codec| Codec::from_str(codec).ok())
        {
            codec
        } else {
            Codec::Null
        };
        let marker = read_file_marker(reader)?;

        Ok((schema, codec, marker))
    } else {
        Err(Error::GetHeaderMetadata)
    }
}
