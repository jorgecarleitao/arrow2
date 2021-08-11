use std::io::{Read, Seek};
use std::{collections::VecDeque, convert::TryInto};

use crate::datatypes::DataType;
use crate::error::Result;
use crate::io::ipc::gen::Message::BodyCompression;
use crate::{array::PrimitiveArray, types::NativeType};

use super::super::super::gen;
use super::super::deserialize::Node;
use super::super::read_basic::*;

pub fn read_primitive<T: NativeType, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<BodyCompression>,
) -> Result<PrimitiveArray<T>>
where
    Vec<u8>: TryInto<T::Bytes>,
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

    let values = read_buffer(
        buffers,
        field_node.length() as usize,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )?;
    Ok(PrimitiveArray::<T>::from_data(data_type, values, validity))
}

pub fn skip_primitive(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
) {
    let _ = field_nodes.pop_front().unwrap();

    let _ = buffers.pop_front().unwrap();
    let _ = buffers.pop_front().unwrap();
}
