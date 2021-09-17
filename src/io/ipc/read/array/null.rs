use std::collections::VecDeque;

use crate::{array::NullArray, datatypes::DataType};

use super::super::deserialize::Node;

pub fn read_null(field_nodes: &mut VecDeque<Node>, data_type: DataType) -> NullArray {
    NullArray::from_data(
        data_type,
        field_nodes.pop_front().unwrap().0.length() as usize,
    )
}

pub fn skip_null(field_nodes: &mut VecDeque<Node>) {
    let _ = field_nodes.pop_front();
}
