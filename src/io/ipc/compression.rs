use crate::error::Result;

#[cfg(feature = "io_ipc_compression")]
pub fn decompress_lz4(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    use std::io::Read;
    let mut decoder = lz4::Decoder::new(input_buf)?;
    decoder.read_exact(output_buf).map_err(|e| e.into())
}

#[cfg(feature = "io_ipc_compression")]
pub fn decompress_zstd(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    use std::io::Read;
    let mut decoder = zstd::Decoder::new(input_buf)?;
    decoder.read_exact(output_buf).map_err(|e| e.into())
}

#[cfg(not(feature = "io_ipc_compression"))]
pub fn decompress_lz4(_input_buf: &[u8], _output_buf: &mut [u8]) -> Result<()> {
    use crate::error::ArrowError;
    Err(ArrowError::Ipc("The crate was compiled without IPC compression. Use `io_ipc_compression` to read compressed IPC.".to_string()))
}

#[cfg(not(feature = "io_ipc_compression"))]
pub fn decompress_zstd(_input_buf: &[u8], _output_buf: &mut [u8]) -> Result<()> {
    use crate::error::ArrowError;
    Err(ArrowError::Ipc("The crate was compiled without IPC compression. Use `io_ipc_compression` to read compressed IPC.".to_string()))
}
