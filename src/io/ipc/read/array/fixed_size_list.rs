use std::collections::VecDeque;
use std::io::{Read, Seek};

use crate::array::FixedSizeListArray;
use crate::datatypes::DataType;
use crate::error::{Error, Result};

use super::super::super::IpcField;
use super::super::deserialize::{read, skip};
use super::super::read_basic::*;
use super::super::{Compression, Dictionaries, IpcBuffer, Node, ReadBuffer, Version};

#[allow(clippy::too_many_arguments)]
pub fn read_fixed_size_list<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    ipc_field: &IpcField,
    buffers: &mut VecDeque<IpcBuffer>,
    reader: &mut R,
    dictionaries: &Dictionaries,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<Compression>,
    version: Version,
    scratch: &mut ReadBuffer,
) -> Result<FixedSizeListArray> {
    let field_node = field_nodes.pop_front().ok_or_else(|| {
        Error::oos(format!(
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

    let (field, _) = FixedSizeListArray::get_child_and_size(&data_type);

    let values = read(
        field_nodes,
        field,
        &ipc_field.fields[0],
        buffers,
        reader,
        dictionaries,
        block_offset,
        is_little_endian,
        compression,
        version,
        scratch,
    )?;
    FixedSizeListArray::try_new(data_type, values, validity)
}

pub fn skip_fixed_size_list(
    field_nodes: &mut VecDeque<Node>,
    data_type: &DataType,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<()> {
    let _ = field_nodes.pop_front().ok_or_else(|| {
        Error::oos(
            "IPC: unable to fetch the field for fixed-size list. The file or stream is corrupted.",
        )
    })?;

    let _ = buffers
        .pop_front()
        .ok_or_else(|| Error::oos("IPC: missing validity buffer."))?;

    let (field, _) = FixedSizeListArray::get_child_and_size(data_type);

    skip(field_nodes, field.data_type(), buffers)
}
