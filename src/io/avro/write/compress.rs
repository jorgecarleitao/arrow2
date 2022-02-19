//! APIs to read from Avro format to arrow.

use crate::error::Result;

use super::Compression;
use super::{Block, CompressedBlock};

const CRC_TABLE: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

/// Compresses a [`Block`] to a [`CompressedBlock`].
pub fn compress(
    block: &mut Block,
    compressed: &mut CompressedBlock,
    compression: Option<Compression>,
) -> Result<bool> {
    compressed.number_of_rows = block.number_of_rows;
    let block = &mut block.data;
    let compressed = &mut compressed.data;

    match compression {
        None => {
            std::mem::swap(block, compressed);
            Ok(true)
        }
        #[cfg(feature = "io_avro_compression")]
        Some(Compression::Deflate) => {
            use std::io::Write;
            compressed.clear();
            let mut encoder = libflate::deflate::Encoder::new(compressed);
            encoder.write_all(block)?;
            encoder.finish();
            Ok(false)
        }
        #[cfg(feature = "io_avro_compression")]
        Some(Compression::Snappy) => {
            use snap::raw::{max_compress_len, Encoder};

            compressed.clear();

            let required_len = max_compress_len(block.len());
            compressed.resize(required_len, 0);
            let compressed_bytes = Encoder::new()
                .compress(block, compressed)
                .map_err(|e| crate::error::ArrowError::ExternalFormat(e.to_string()))?;
            compressed.truncate(compressed_bytes);

            compressed.extend(CRC_TABLE.checksum(block).to_be_bytes());
            Ok(false)
        }
        #[cfg(not(feature = "io_avro_compression"))]
        Some(Compression::Deflate) => Err(crate::error::ArrowError::InvalidArgumentError(
            "Trying to compress Avro with deflate but feature 'io_avro_compression' is not active."
                .to_string(),
        )),
        #[cfg(not(feature = "io_avro_compression"))]
        Some(Compression::Snappy) => Err(crate::error::ArrowError::InvalidArgumentError(
            "Trying to compress Avro with snappy but feature 'io_avro_compression' is not active."
                .to_string(),
        )),
    }
}
