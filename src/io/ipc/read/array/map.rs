use std::collections::{HashMap, VecDeque};
use std::io::{Read, Seek};
use std::sync::Arc;

use arrow_format::ipc;

use crate::array::{Array, MapArray};
use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::error::Result;

use super::super::deserialize::{read, skip, Node};
use super::super::read_basic::*;

#[allow(clippy::too_many_arguments)]
pub fn read_map<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&ipc::Schema::Buffer>,
    reader: &mut R,
    dictionaries: &HashMap<usize, Arc<dyn Array>>,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<ipc::Message::BodyCompression>,
    version: ipc::Schema::MetadataVersion,
) -> Result<MapArray> {
    let field_node = field_nodes.pop_front().unwrap();

    let validity = read_validity(
        buffers,
        field_node,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )?;

    let offsets = read_buffer::<i32, _>(
        buffers,
        1 + field_node.length() as usize,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )
    // Older versions of the IPC format sometimes do not report an offset
    .or_else(|_| Result::Ok(Buffer::<i32>::from(&[0i32])))?;

    let field = MapArray::get_field(&data_type);

    let field = read(
        field_nodes,
        field,
        buffers,
        reader,
        dictionaries,
        block_offset,
        is_little_endian,
        compression,
        version,
    )?;
    Ok(MapArray::from_data(data_type, offsets, field, validity))
}

pub fn skip_map(
    field_nodes: &mut VecDeque<Node>,
    data_type: &DataType,
    buffers: &mut VecDeque<&ipc::Schema::Buffer>,
) {
    let _ = field_nodes.pop_front().unwrap();

    let _ = buffers.pop_front().unwrap();
    let _ = buffers.pop_front().unwrap();

    let data_type = MapArray::get_field(data_type).data_type();

    skip(field_nodes, data_type, buffers)
}
