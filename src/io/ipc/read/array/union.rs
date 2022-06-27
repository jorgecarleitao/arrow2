use std::collections::VecDeque;
use std::io::{Read, Seek};

use crate::array::UnionArray;
use crate::datatypes::DataType;
use crate::datatypes::UnionMode::Dense;
use crate::error::{Error, Result};
use crate::io::ipc::read::common::ReadBuffer;

use super::super::super::IpcField;
use super::super::deserialize::{read, skip};
use super::super::read_basic::*;
use super::super::Dictionaries;
use super::super::{Compression, IpcBuffer, Node, OutOfSpecKind, Version};

#[allow(clippy::too_many_arguments)]
pub fn read_union<R: Read + Seek>(
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
) -> Result<UnionArray> {
    let field_node = field_nodes.pop_front().ok_or_else(|| {
        Error::oos(format!(
            "IPC: unable to fetch the field for {:?}. The file or stream is corrupted.",
            data_type
        ))
    })?;

    if version != Version::V5 {
        let _ = buffers
            .pop_front()
            .ok_or_else(|| Error::oos("IPC: missing validity buffer."))?;
    };

    let length: usize = field_node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;

    let types = read_buffer(
        buffers,
        length,
        reader,
        block_offset,
        is_little_endian,
        compression,
        scratch,
    )?;

    let offsets = if let DataType::Union(_, _, mode) = data_type {
        if !mode.is_sparse() {
            Some(read_buffer(
                buffers,
                length,
                reader,
                block_offset,
                is_little_endian,
                compression,
                scratch,
            )?)
        } else {
            None
        }
    } else {
        unreachable!()
    };

    let fields = UnionArray::get_fields(&data_type);

    let fields = fields
        .iter()
        .zip(ipc_field.fields.iter())
        .map(|(field, ipc_field)| {
            read(
                field_nodes,
                field,
                ipc_field,
                buffers,
                reader,
                dictionaries,
                block_offset,
                is_little_endian,
                compression,
                version,
                scratch,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    UnionArray::try_new(data_type, types, fields, offsets)
}

pub fn skip_union(
    field_nodes: &mut VecDeque<Node>,
    data_type: &DataType,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<()> {
    let _ = field_nodes.pop_front().ok_or_else(|| {
        Error::oos("IPC: unable to fetch the field for struct. The file or stream is corrupted.")
    })?;

    let _ = buffers
        .pop_front()
        .ok_or_else(|| Error::oos("IPC: missing validity buffer."))?;
    if let DataType::Union(_, _, Dense) = data_type {
        let _ = buffers
            .pop_front()
            .ok_or_else(|| Error::oos("IPC: missing offsets buffer."))?;
    } else {
        unreachable!()
    };

    let fields = UnionArray::get_fields(data_type);

    fields
        .iter()
        .try_for_each(|field| skip(field_nodes, field.data_type(), buffers))
}
