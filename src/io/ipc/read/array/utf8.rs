use std::collections::VecDeque;
use std::io::{Read, Seek};

use crate::array::{Offset, Utf8Array};
use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};

use super::super::read_basic::*;
use super::super::{Compression, IpcBuffer, Node};

pub fn read_utf8<O: Offset, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<IpcBuffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<Compression>,
) -> Result<Utf8Array<O>> {
    let field_node = field_nodes.pop_front().ok_or_else(|| {
        ArrowError::oos(format!(
            "IPC: unable to fetch the field for {:?}. The file or stream is corrupted.",
            data_type
        ))
    })?;

    let validity = read_validity(
        buffers,
        field_node,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )?;

    let offsets: Buffer<O> = read_buffer(
        buffers,
        1 + field_node.length() as usize,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )
    // Older versions of the IPC format sometimes do not report an offset
    .or_else(|_| Result::Ok(Buffer::<O>::from(vec![O::default()])))?;

    let last_offset = offsets.as_slice()[offsets.len() - 1].to_usize();
    let values = read_buffer(
        buffers,
        last_offset,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )?;

    Ok(Utf8Array::<O>::from_data(
        data_type, offsets, values, validity,
    ))
}

pub fn skip_utf8(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<()> {
    let _ = field_nodes.pop_front().ok_or_else(|| {
        ArrowError::oos("IPC: unable to fetch the field for utf8. The file or stream is corrupted.")
    })?;

    let _ = buffers
        .pop_front()
        .ok_or_else(|| ArrowError::oos("IPC: missing validity buffer."))?;
    let _ = buffers
        .pop_front()
        .ok_or_else(|| ArrowError::oos("IPC: missing offsets buffer."))?;
    let _ = buffers
        .pop_front()
        .ok_or_else(|| ArrowError::oos("IPC: missing values buffer."))?;
    Ok(())
}
