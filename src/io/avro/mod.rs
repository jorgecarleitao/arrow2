#![deny(missing_docs)]
#![forbid(unsafe_code)]
//! Read and write from and to Apache Avro

pub mod read;
#[cfg(feature = "io_avro_async")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_avro_async")))]
pub mod read_async;

// macros that can operate in sync and async code.
macro_rules! avro_decode {
    ($reader:ident $($_await:tt)*) => {
        {
            let mut i = 0u64;
            let mut buf = [0u8; 1];

            let mut j = 0;
            loop {
                if j > 9 {
                    // if j * 7 > 64
                    return Err(ArrowError::ExternalFormat(
                        "zigzag decoding failed - corrupt avro file".to_string(),
                    ));
                }
                $reader.read_exact(&mut buf[..])$($_await)*?;
                i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
                if (buf[0] >> 7) == 0 {
                    break;
                } else {
                    j += 1;
                }
            }

            Ok(i)
        }
    }
}

macro_rules! read_header {
    ($reader:ident $($_await:tt)*) => {{
        let mut items = HashMap::new();

        loop {
            let len = zigzag_i64($reader)$($_await)*? as usize;
            if len == 0 {
                break Ok(items);
            }

            items.reserve(len);
            for _ in 0..len {
                let key = _read_binary($reader)$($_await)*?;
                let key = String::from_utf8(key)
                    .map_err(|_| ArrowError::ExternalFormat("Invalid Avro header".to_string()))?;
                let value = _read_binary($reader)$($_await)*?;
                items.insert(key, value);
            }
        }
    }};
}

macro_rules! read_metadata {
    ($reader:ident $($_await:tt)*) => {{
        let mut magic_number = [0u8; 4];
        $reader.read_exact(&mut magic_number)$($_await)*?;

        // see https://avro.apache.org/docs/current/spec.html#Object+Container+Files
        if magic_number != [b'O', b'b', b'j', 1u8] {
            return Err(ArrowError::ExternalFormat(
                "Avro header does not contain a valid magic number".to_string(),
            ));
        }

        let header = read_header($reader)$($_await)*?;

        let (schema, compression) = deserialize_header(header)?;

        let marker = read_file_marker($reader)$($_await)*?;

        Ok((schema, compression, marker))
    }};
}

pub(crate) use {avro_decode, read_header, read_metadata};
