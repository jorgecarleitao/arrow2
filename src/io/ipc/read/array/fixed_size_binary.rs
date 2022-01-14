use std::collections::VecDeque;
use std::io::{Read, Seek};

use crate::array::FixedSizeBinaryArray;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};

use super::super::read_basic::*;
use super::super::{Compression, IpcBuffer, Node};

pub fn read_fixed_size_binary<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<IpcBuffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<Compression>,
) -> Result<FixedSizeBinaryArray> {
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

    let length = field_node.length() as usize * FixedSizeBinaryArray::get_size(&data_type);
    let values = read_buffer(
        buffers,
        length,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )?;

    Ok(FixedSizeBinaryArray::from_data(data_type, values, validity))
}

pub fn skip_fixed_size_binary(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<()> {
    let _ = field_nodes.pop_front().ok_or_else(|| {
        ArrowError::oos(
            "IPC: unable to fetch the field for fixed-size binary. The file or stream is corrupted.",
        )
    })?;

    let _ = buffers
        .pop_front()
        .ok_or_else(|| ArrowError::oos("IPC: missing validity buffer."))?;
    let _ = buffers
        .pop_front()
        .ok_or_else(|| ArrowError::oos("IPC: missing values buffer."))?;
    Ok(())
}
