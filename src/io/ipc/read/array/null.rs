use std::collections::VecDeque;

use crate::array::NullArray;

use super::super::deserialize::Node;

pub fn read_null(field_nodes: &mut VecDeque<Node>) -> NullArray {
    NullArray::from_data(field_nodes.pop_front().unwrap().0.length() as usize)
}

pub fn skip_null(field_nodes: &mut VecDeque<Node>) {
    let _ = field_nodes.pop_front();
}
