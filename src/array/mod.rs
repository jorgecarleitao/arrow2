use std::any::Any;

use crate::datatypes::DataType;

pub trait Array: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn len(&self) -> usize;

    fn data_type(&self) -> &DataType;

    fn is_null(&self, index: usize) -> bool;
}

mod binary;
mod dictionary;
mod fixed_binary;
mod list;
mod primitive;
mod specification;
