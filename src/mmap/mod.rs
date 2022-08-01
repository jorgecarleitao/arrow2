//! Memory maps regions defined on the IPC format into [`Array`].
use std::collections::VecDeque;

use crate::array::Array;
use crate::error::Error;
use crate::ffi::mmap;

use crate::io::ipc::read::read_file_metadata;
use crate::io::ipc::read::reader::get_serialized_batch;
use crate::io::ipc::read::OutOfSpecKind;
use crate::io::ipc::CONTINUATION_MARKER;

use arrow_format::ipc::planus::ReadAsRoot;

/// something
/// # Safety
/// This operation is innerently unsafe as it assumes that `T` contains valid Arrow data
/// In particular:
/// * Offsets in variable-sized containers are valid;
/// * Utf8 is valid
pub unsafe fn map_chunk_unchecked<T: Clone + AsRef<[u8]>>(
    data: T,
    index: usize,
) -> Result<Box<dyn Array>, Error> {
    let mut bytes = data.as_ref();
    let metadata = read_file_metadata(&mut std::io::Cursor::new(bytes))?;

    let block = metadata.blocks[index];

    let offset: usize = block
        .offset
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let meta_data_length: usize = block
        .meta_data_length
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    bytes = &bytes[offset..];
    let mut message_length = bytes[..4].try_into().unwrap();
    bytes = &bytes[4..];

    if message_length == CONTINUATION_MARKER {
        // continuation marker encountered, read message next
        message_length = bytes[..4].try_into().unwrap();
        bytes = &bytes[4..];
    };

    let message_length: usize = i32::from_le_bytes(message_length)
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let message = arrow_format::ipc::MessageRef::read_as_root(&bytes[..message_length])
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferMessage(err)))?;

    let batch = get_serialized_batch(&message)?;

    let buffers = batch
        .buffers()
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferBuffers(err)))?
        .ok_or_else(|| Error::from(OutOfSpecKind::MissingMessageBuffers))?;
    let mut buffers = buffers.iter().collect::<VecDeque<_>>();

    let field_nodes = batch
        .nodes()
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferNodes(err)))?
        .ok_or_else(|| Error::from(OutOfSpecKind::MissingMessageNodes))?;
    let mut field_nodes = field_nodes.iter().collect::<VecDeque<_>>();

    println!("{:#?}", metadata.schema.fields);
    let data_type = metadata.schema.fields[0].data_type.clone();
    println!("{:#?}", data_type);

    mmap::mmap(
        data.clone(),
        offset + meta_data_length,
        data_type,
        &mut field_nodes,
        &mut buffers,
    )
}
