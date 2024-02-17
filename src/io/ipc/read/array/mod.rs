mod primitive;

use std::collections::VecDeque;

pub use primitive::*;
mod boolean;
pub use boolean::*;
mod utf8;
pub use utf8::*;
mod binary;
pub use binary::*;
mod fixed_size_binary;
pub use fixed_size_binary::*;
mod list;
pub use list::*;
mod fixed_size_list;
pub use fixed_size_list::*;
mod struct_;
pub use struct_::*;
mod null;
pub use null::*;
mod dictionary;
pub use dictionary::*;
mod union;
pub use union::*;
mod binview;
mod map;
pub use binview::*;
pub use map::*;

use super::{Compression, IpcBuffer, Node, OutOfSpecKind};
use crate::datatypes::DataType;
use crate::error::{Error, Result};

fn try_get_field_node<'a>(
    field_nodes: &mut VecDeque<Node<'a>>,
    data_type: &DataType,
) -> Result<Node<'a>> {
    field_nodes.pop_front().ok_or_else(|| {
        Error::oos(format!("IPC: unable to fetch the field for {:?}\n\nThe file or stream is corrupted.", data_type))
    })
}

fn try_get_array_length(field_node: Node, limit: Option<usize>) -> Result<usize> {
    let length: usize = field_node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;
    Ok(limit.map(|limit| limit.min(length)).unwrap_or(length))
}
