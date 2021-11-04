use std::collections::VecDeque;
use std::io::{Read, Seek};

use arrow_format::ipc;

use crate::array::BooleanArray;
use crate::datatypes::DataType;
use crate::error::Result;

use super::super::deserialize::Node;
use super::super::read_basic::*;

pub fn read_boolean<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&ipc::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<ipc::Message::BodyCompression>,
) -> Result<BooleanArray> {
    let field_node = field_nodes.pop_front().unwrap().0;

    let length = field_node.length() as usize;
    let validity = read_validity(
        buffers,
        field_node,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )?;

    let values = read_bitmap(
        buffers,
        length,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )?;
    Ok(BooleanArray::from_data(data_type, values, validity))
}

pub fn skip_boolean(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<&ipc::Schema::Buffer>,
) {
    let _ = field_nodes.pop_front().unwrap();

    let _ = buffers.pop_front().unwrap();
    let _ = buffers.pop_front().unwrap();
}
