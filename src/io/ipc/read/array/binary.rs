use std::collections::VecDeque;
use std::io::{Read, Seek};

use crate::array::BinaryArray;
use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::error::{Error, Result};
use crate::io::ipc::read::array::{try_get_array_length, try_get_field_node};
use crate::offset::Offset;

use super::super::read_basic::*;
use super::super::{Compression, IpcBuffer, Node, OutOfSpecKind};

#[allow(clippy::too_many_arguments)]
pub fn read_binary<O: Offset, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<IpcBuffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<Compression>,
    limit: Option<usize>,
    scratch: &mut Vec<u8>,
) -> Result<BinaryArray<O>> {
    let field_node = try_get_field_node(field_nodes, &data_type)?;

    let validity = read_validity(
        buffers,
        field_node,
        reader,
        block_offset,
        is_little_endian,
        compression,
        limit,
        scratch,
    )?;

    let length = try_get_array_length(field_node, limit)?;

    let offsets: Buffer<O> = read_buffer(
        buffers,
        1 + length,
        reader,
        block_offset,
        is_little_endian,
        compression,
        scratch,
    )
    // Older versions of the IPC format sometimes do not report an offset
    .or_else(|_| Result::Ok(Buffer::<O>::from(vec![O::default()])))?;

    let last_offset = offsets.last().unwrap().to_usize();
    let values = read_buffer(
        buffers,
        last_offset,
        reader,
        block_offset,
        is_little_endian,
        compression,
        scratch,
    )?;

    BinaryArray::<O>::try_new(data_type, offsets.try_into()?, values, validity)
}

pub fn skip_binary(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<()> {
    let _ = field_nodes.pop_front().ok_or_else(|| {
        Error::oos("IPC: unable to fetch the field for binary. The file or stream is corrupted.")
    })?;

    let _ = buffers
        .pop_front()
        .ok_or_else(|| Error::oos("IPC: missing validity buffer."))?;
    let _ = buffers
        .pop_front()
        .ok_or_else(|| Error::oos("IPC: missing offsets buffer."))?;
    let _ = buffers
        .pop_front()
        .ok_or_else(|| Error::oos("IPC: missing values buffer."))?;
    Ok(())
}
