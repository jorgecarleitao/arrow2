use std::collections::{HashSet, VecDeque};
use std::convert::TryInto;
use std::io::{Read, Seek};

use crate::array::{DictionaryArray, DictionaryKey};
use crate::error::{Error, Result};

use super::super::Dictionaries;
use super::super::{Compression, IpcBuffer, Node};
use super::{read_primitive, skip_primitive};

#[allow(clippy::too_many_arguments)]
pub fn read_dictionary<T: DictionaryKey, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    id: Option<i64>,
    buffers: &mut VecDeque<IpcBuffer>,
    reader: &mut R,
    dictionaries: &Dictionaries,
    block_offset: u64,
    compression: Option<Compression>,
    is_little_endian: bool,
) -> Result<DictionaryArray<T>>
where
    Vec<u8>: TryInto<T::Bytes>,
{
    let id = if let Some(id) = id {
        id
    } else {
        return Err(Error::OutOfSpec("Dictionary has no id.".to_string()));
    };
    let values = dictionaries
        .get(&id)
        .ok_or_else(|| {
            let valid_ids = dictionaries.keys().collect::<HashSet<_>>();
            Error::OutOfSpec(format!(
                "Dictionary id {} not found. Valid ids: {:?}",
                id, valid_ids
            ))
        })?
        .clone();

    let keys = read_primitive(
        field_nodes,
        T::PRIMITIVE.into(),
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
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<()> {
    skip_primitive(field_nodes, buffers)
}
