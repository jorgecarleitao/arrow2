use std::collections::VecDeque;
use std::io::{Read, Seek};

use arrow_format::ipc;

use crate::array::{Offset, Utf8Array};
use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::error::Result;

use super::super::deserialize::Node;
use super::super::read_basic::*;

pub fn read_utf8<O: Offset, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&ipc::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<ipc::Message::BodyCompression>,
) -> Result<Utf8Array<O>> {
    let field_node = field_nodes.pop_front().unwrap();

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
    .or_else(|_| Result::Ok(Buffer::<O>::from(&[O::default()])))?;

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

pub fn skip_utf8(field_nodes: &mut VecDeque<Node>, buffers: &mut VecDeque<&ipc::Schema::Buffer>) {
    let _ = field_nodes.pop_front().unwrap();

    let _ = buffers.pop_front().unwrap();
    let _ = buffers.pop_front().unwrap();
    let _ = buffers.pop_front().unwrap();
}
