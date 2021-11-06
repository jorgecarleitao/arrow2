use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::io::{Read, Seek};
use std::sync::Arc;

use arrow_format::ipc;

use crate::array::{Array, DictionaryArray, DictionaryKey};
use crate::datatypes::Field;
use crate::error::Result;

use super::super::deserialize::Node;
use super::{read_primitive, skip_primitive};

#[allow(clippy::too_many_arguments)]
pub fn read_dictionary<T: DictionaryKey, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    field: &Field,
    buffers: &mut VecDeque<&ipc::Schema::Buffer>,
    reader: &mut R,
    dictionaries: &HashMap<usize, Arc<dyn Array>>,
    block_offset: u64,
    compression: Option<ipc::Message::BodyCompression>,
    is_little_endian: bool,
) -> Result<DictionaryArray<T>>
where
    Vec<u8>: TryInto<T::Bytes>,
{
    let values = dictionaries
        .get(&(field.dict_id().unwrap() as usize))
        .unwrap()
        .clone();

    let keys = read_primitive(
        field_nodes,
        T::DATA_TYPE,
        buffers,
        reader,
        block_offset,
        is_little_endian,
        compression,
    )?;

    Ok(DictionaryArray::<T>::from_data(keys, values))
}

pub fn skip_dictionary(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<&ipc::Schema::Buffer>,
) {
    skip_primitive(field_nodes, buffers)
}
