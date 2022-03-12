use std::sync::Arc;

use crate::array::{Array, StructArray};
use crate::datatypes::{DataType, Field};
use crate::error::ArrowError;

use super::nested_utils::{NestedArrayIter, NestedState};

pub struct StructIterator<'a> {
    iters: Vec<NestedArrayIter<'a>>,
    fields: Vec<Field>,
}

impl<'a> StructIterator<'a> {
    pub fn new(iters: Vec<NestedArrayIter<'a>>, fields: Vec<Field>) -> Self {
        assert_eq!(iters.len(), fields.len());
        Self { iters, fields }
    }
}

impl<'a> Iterator for StructIterator<'a> {
    type Item = Result<(NestedState, Arc<dyn Array>), ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let values = self
            .iters
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Vec<_>>();

        if values.iter().any(|x| x.is_none()) {
            return None;
        }
        let values = values
            .into_iter()
            .map(|x| x.unwrap().map(|x| x.1))
            .collect::<Result<Vec<_>, ArrowError>>();

        match values {
            Ok(values) => Some(Ok((
                NestedState::new(vec![]), // todo
                Arc::new(StructArray::from_data(
                    DataType::Struct(self.fields.clone()),
                    values,
                    None,
                )),
            ))),
            Err(e) => Some(Err(e)),
        }
    }
}
