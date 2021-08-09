use std::collections::VecDeque;
use std::convert::TryInto;
use std::io::{Read, Seek};

use crate::array::{DictionaryArray, DictionaryKey};
use crate::error::Result;

use super::super::super::gen;
use super::super::deserialize::Node;
use super::{read_primitive, skip_primitive};

pub fn read_dictionary<T: DictionaryKey, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
) -> Result<DictionaryArray<T>>
where
    Vec<u8>: TryInto<T::Bytes>,
{
    let values = field_nodes.front().unwrap().1.as_ref().unwrap();

    let keys = read_primitive(
        field_nodes,
        T::DATA_TYPE,
        buffers,
        reader,
        block_offset,
        is_little_endian,
        None,
    )?;

    Ok(DictionaryArray::<T>::from_data(keys, values.clone()))
}

pub fn skip_dictionary(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<&gen::Schema::Buffer>,
) {
    skip_primitive(field_nodes, buffers)
}
