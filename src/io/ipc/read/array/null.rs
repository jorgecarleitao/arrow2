use std::collections::VecDeque;

use crate::{
    array::NullArray,
    datatypes::DataType,
    error::{Error, Result},
};

use super::super::Node;
use crate::io::ipc::read::array::{try_get_array_length, try_get_field_node};

pub fn read_null(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    limit: Option<usize>,
) -> Result<NullArray> {
    let field_node = try_get_field_node(field_nodes, &data_type)?;

    let length = try_get_array_length(field_node, limit)?;

    NullArray::try_new(data_type, length)
}

pub fn skip_null(field_nodes: &mut VecDeque<Node>) -> Result<()> {
    let _ = field_nodes.pop_front().ok_or_else(|| {
        Error::oos("IPC: unable to fetch the field for null. The file or stream is corrupted.")
    })?;
    Ok(())
}
