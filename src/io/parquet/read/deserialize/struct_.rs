use std::sync::Arc;

use crate::array::{Array, StructArray};
use crate::datatypes::{DataType, Field};
use crate::error::Error;

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
    type Item = Result<(NestedState, Arc<dyn Array>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let values = self
            .iters
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Vec<_>>();

        if values.iter().any(|x| x.is_none()) {
            return None;
        }

        // todo: unzip of Result not yet supportted in stable Rust
        let mut nested = vec![];
        let mut new_values = vec![];
        for x in values {
            match x.unwrap() {
                Ok((nest, values)) => {
                    new_values.push(values);
                    nested.push(nest);
                }
                Err(e) => return Some(Err(e)),
            }
        }
        let mut nested = nested.pop().unwrap();
        let (_, validity) = nested.nested.pop().unwrap().inner();

        Some(Ok((
            nested,
            Arc::new(StructArray::from_data(
                DataType::Struct(self.fields.clone()),
                new_values,
                validity.and_then(|x| x.into()),
            )),
        )))
    }
}
