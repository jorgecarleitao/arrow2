use std::collections::VecDeque;

use crate::{
    array::NullArray,
    datatypes::DataType,
    error::{Error, Result},
};

use super::super::Node;

pub fn read_null(field_nodes: &mut VecDeque<Node>, data_type: DataType) -> Result<NullArray> {
    let field_node = field_nodes.pop_front().ok_or_else(|| {
        Error::oos(format!(
            "IPC: unable to fetch the field for {:?}. The file or stream is corrupted.",
            data_type
        ))
    })?;

    NullArray::try_new(data_type, field_node.length() as usize)
}

pub fn skip_null(field_nodes: &mut VecDeque<Node>) -> Result<()> {
    let _ = field_nodes.pop_front().ok_or_else(|| {
        Error::oos("IPC: unable to fetch the field for null. The file or stream is corrupted.")
    })?;
    Ok(())
}
