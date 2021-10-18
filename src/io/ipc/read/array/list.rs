use std::collections::VecDeque;
use std::convert::TryInto;
use std::io::{Read, Seek};

use arrow_format::ipc;

use crate::array::{ListArray, Offset};
use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::error::Result;

use super::super::deserialize::{read, skip, Node};
use super::super::read_basic::*;

#[allow(clippy::too_many_arguments)]
pub fn read_list<O: Offset, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&ipc::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<ipc::Message::BodyCompression>,
    version: ipc::Schema::MetadataVersion,
) -> Result<ListArray<O>>
where
    Vec<u8>: TryInto<O::Bytes>,
{
    let field_node = field_nodes.pop_front().unwrap().0;

    let validity = read_validity(
        buffers,
        field_node,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )?;

    let offsets = read_buffer::<O, _>(
        buffers,
        1 + field_node.length() as usize,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )
    // Older versions of the IPC format sometimes do not report an offset
    .or_else(|_| Result::Ok(Buffer::<O>::from(&[O::default()])))?;

    let value_data_type = ListArray::<O>::get_child_type(&data_type).clone();

    let values = read(
        field_nodes,
        value_data_type,
        buffers,
        reader,
        block_offset,
        is_little_endian,
        compression,
        version,
    )?;
    Ok(ListArray::from_data(data_type, offsets, values, validity))
}

pub fn skip_list<O: Offset>(
    field_nodes: &mut VecDeque<Node>,
    data_type: &DataType,
    buffers: &mut VecDeque<&ipc::Schema::Buffer>,
) {
    let _ = field_nodes.pop_front().unwrap();

    let _ = buffers.pop_front().unwrap();
    let _ = buffers.pop_front().unwrap();

    let data_type = ListArray::<O>::get_child_type(data_type);

    skip(field_nodes, data_type, buffers)
}
