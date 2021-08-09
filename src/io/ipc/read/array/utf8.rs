use std::collections::VecDeque;
use std::convert::TryInto;
use std::io::{Read, Seek};

use crate::array::{Offset, Utf8Array};
use crate::buffer::Buffer;
use crate::error::Result;
use crate::io::ipc::gen::Message::BodyCompression;
use crate::types::NativeType;

use super::super::super::gen;
use super::super::deserialize::Node;
use super::super::read_basic::*;

pub fn read_utf8<O: Offset, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<BodyCompression>,
) -> Result<Utf8Array<O>>
where
    Vec<u8>: TryInto<O::Bytes> + TryInto<<u8 as NativeType>::Bytes>,
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

    Ok(Utf8Array::<O>::from_data(offsets, values, validity))
}

pub fn skip_utf8(field_nodes: &mut VecDeque<Node>, buffers: &mut VecDeque<&gen::Schema::Buffer>) {
    let _ = field_nodes.pop_front().unwrap();

    let _ = buffers.pop_front().unwrap();
    let _ = buffers.pop_front().unwrap();
    let _ = buffers.pop_front().unwrap();
}
