//! Memory maps regions defined on the IPC format into [`Array`].
use std::collections::VecDeque;

use crate::array::Array;
use crate::chunk::Chunk;
use crate::error::Error;
use crate::ffi::mmap;

use crate::io::ipc::read::reader::get_serialized_batch;
use crate::io::ipc::read::FileMetadata;
use crate::io::ipc::read::{IpcBuffer, Node, OutOfSpecKind};
use crate::io::ipc::CONTINUATION_MARKER;

use arrow_format::ipc::planus::ReadAsRoot;
use arrow_format::ipc::MessageRef;

fn read_message(
    mut bytes: &[u8],
    block: arrow_format::ipc::Block,
) -> Result<(MessageRef, usize), Error> {
    let offset: usize = block
        .offset
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let block_length: usize = block
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

    Ok((message, offset + block_length))
}

fn read_batch<'a>(
    message: &'a MessageRef,
) -> Result<(VecDeque<IpcBuffer<'a>>, VecDeque<Node<'a>>), Error> {
    let batch = get_serialized_batch(message)?;

    let compression = batch.compression()?;
    if compression.is_some() {
        return Err(Error::nyi(
            "mmap can only be done on uncompressed IPC files",
        ));
    }

    let buffers = batch
        .buffers()
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferBuffers(err)))?
        .ok_or_else(|| Error::from(OutOfSpecKind::MissingMessageBuffers))?;
    let buffers = buffers.iter().collect::<VecDeque<_>>();

    let field_nodes = batch
        .nodes()
        .map_err(|err| Error::from(OutOfSpecKind::InvalidFlatbufferNodes(err)))?
        .ok_or_else(|| Error::from(OutOfSpecKind::MissingMessageNodes))?;
    let field_nodes = field_nodes.iter().collect::<VecDeque<_>>();

    Ok((buffers, field_nodes))
}

/// Memory maps an record batch from an IPC file into a [`Chunk`].
/// # Errors
/// This function errors when:
/// * The IPC file is not valid
/// * the buffers on the file are un-aligned with their corresponding data. This can happen when:
///     * the file was written with 8-bit alignment
///     * the file contains type decimal 128 or 256
/// # Safety
/// The caller must ensure that `data` contains a valid buffers, for example:
/// * Offsets in variable-sized containers must be in-bounds and increasing
/// * Utf8 data is valid
pub unsafe fn mmap_unchecked<T: Clone + AsRef<[u8]>>(
    metadata: &FileMetadata,
    data: T,
    chunk: usize,
) -> Result<Chunk<Box<dyn Array>>, Error> {
    let block = metadata.blocks[chunk];
    let (message, offset) = read_message(data.as_ref(), block)?;
    let (mut buffers, mut field_nodes) = read_batch(&message)?;

    metadata
        .schema
        .fields
        .iter()
        .map(|f| &f.data_type)
        .cloned()
        .map(|data_type| {
            mmap::mmap(
                data.clone(),
                offset,
                data_type,
                &mut field_nodes,
                &mut buffers,
            )
        })
        .collect::<Result<_, Error>>()
        .and_then(Chunk::try_new)
}
