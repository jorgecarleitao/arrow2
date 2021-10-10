// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Seek};
use std::sync::Arc;

use arrow_format::ipc::Schema::MetadataVersion;
use arrow_format::ipc;

use crate::array::*;
use crate::datatypes::{DataType, Field, Schema};
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

use super::deserialize::{read, skip};

type ArrayRef = Arc<dyn Array>;

#[derive(Debug, Eq, PartialEq, Hash)]
enum ProjectionResult<A> {
    Selected(A),
    NotSelected(A),
}

/// An iterator adapter that will return `Some(x)` or `None`
/// # Panics
/// The iterator panics iff the `projection` is not strictly increasing.
struct ProjectionIter<'a, A, I: Iterator<Item = A>> {
    projection: &'a [usize],
    iter: I,
    current_count: usize,
    current_projection: usize,
}

impl<'a, A, I: Iterator<Item = A>> ProjectionIter<'a, A, I> {
    /// # Panics
    /// iff `projection` is empty
    pub fn new(projection: &'a [usize], iter: I) -> Self {
        Self {
            projection: &projection[1..],
            iter,
            current_count: 0,
            current_projection: projection[0],
        }
    }
}

impl<'a, A, I: Iterator<Item = A>> Iterator for ProjectionIter<'a, A, I> {
    type Item = ProjectionResult<A>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.iter.next() {
            let result = if self.current_count == self.current_projection {
                if !self.projection.is_empty() {
                    assert!(self.projection[0] > self.current_projection);
                    self.current_projection = self.projection[0];
                    self.projection = &self.projection[1..];
                } else {
                    self.current_projection = 0 // a value that most likely already passed
                };
                Some(ProjectionResult::Selected(item))
            } else {
                Some(ProjectionResult::NotSelected(item))
            };
            self.current_count += 1;
            result
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

/// Creates a record batch from binary data using the `ipc::RecordBatch` indexes and the `Schema`
/// # Panic
/// Panics iff the projection is not in increasing order (e.g. `[1, 0]` nor `[0, 1, 1]` are valid)
#[allow(clippy::too_many_arguments)]
pub fn read_record_batch<R: Read + Seek>(
    batch: ipc::Message::RecordBatch,
    schema: Arc<Schema>,
    projection: Option<(&[usize], Arc<Schema>)>,
    is_little_endian: bool,
    dictionaries: &[Option<ArrayRef>],
    version: MetadataVersion,
    reader: &mut R,
    block_offset: u64,
) -> Result<RecordBatch> {
    let buffers = batch
        .buffers()
        .ok_or_else(|| ArrowError::Ipc("Unable to get buffers from IPC RecordBatch".to_string()))?;
    let mut buffers: VecDeque<&ipc::Schema::Buffer> = buffers.iter().collect();
    let field_nodes = batch.nodes().ok_or_else(|| {
        ArrowError::Ipc("Unable to get field nodes from IPC RecordBatch".to_string())
    })?;

    // This is a bug fix: we should have one dictionary per node, not schema field
    let dictionaries = dictionaries.iter().chain(std::iter::repeat(&None));

    let mut field_nodes = field_nodes
        .iter()
        .zip(dictionaries)
        .collect::<VecDeque<_>>();

    let (schema, columns) = if let Some(projection) = projection {
        let projected_schema = projection.1.clone();

        let projection = ProjectionIter::new(projection.0, schema.fields().iter());

        let arrays = projection
            .map(|maybe_field| match maybe_field {
                ProjectionResult::Selected(field) => Some(read(
                    &mut field_nodes,
                    field.data_type().clone(),
                    &mut buffers,
                    reader,
                    block_offset,
                    is_little_endian,
                    batch.compression(),
                    version,
                )),
                ProjectionResult::NotSelected(field) => {
                    skip(&mut field_nodes, field.data_type(), &mut buffers);
                    None
                }
            })
            .flatten()
            .collect::<Result<Vec<_>>>()?;
        (projected_schema, arrays)
    } else {
        let arrays = schema
            .fields()
            .iter()
            .map(|field| {
                read(
                    &mut field_nodes,
                    field.data_type().clone(),
                    &mut buffers,
                    reader,
                    block_offset,
                    is_little_endian,
                    batch.compression(),
                    version,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        (schema.clone(), arrays)
    };
    RecordBatch::try_new(schema, columns)
}

/// Read the dictionary from the buffer and provided metadata,
/// updating the `dictionaries_by_field` with the resulting dictionary
pub fn read_dictionary<R: Read + Seek>(
    batch: ipc::Message::DictionaryBatch,
    schema: &Schema,
    is_little_endian: bool,
    dictionaries_by_field: &mut [Option<ArrayRef>],
    reader: &mut R,
    block_offset: u64,
) -> Result<()> {
    if batch.isDelta() {
        return Err(ArrowError::NotYetImplemented(
            "delta dictionary batches not supported".to_string(),
        ));
    }

    let id = batch.id();
    let fields_using_this_dictionary = schema.fields_with_dict_id(id);
    let first_field = fields_using_this_dictionary.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError("dictionary id not found in schema".to_string())
    })?;

    // As the dictionary batch does not contain the type of the
    // values array, we need to retrieve this from the schema.
    // Get an array representing this dictionary's values.
    let dictionary_values: ArrayRef = match first_field.data_type() {
        DataType::Dictionary(_, ref value_type) => {
            // Make a fake schema for the dictionary batch.
            let schema = Arc::new(Schema {
                fields: vec![Field::new("", value_type.as_ref().clone(), false)],
                metadata: HashMap::new(),
            });
            // Read a single column
            let record_batch = read_record_batch(
                batch.data().unwrap(),
                schema,
                None,
                is_little_endian,
                dictionaries_by_field,
                MetadataVersion::V5,
                reader,
                block_offset,
            )?;
            Some(record_batch.column(0).clone())
        }
        _ => None,
    }
    .ok_or_else(|| {
        ArrowError::InvalidArgumentError("dictionary id not found in schema".to_string())
    })?;

    // for all fields with this dictionary id, update the dictionaries vector
    // in the reader. Note that a dictionary batch may be shared between many fields.
    // We don't currently record the isOrdered field. This could be general
    // attributes of arrays.
    for (i, field) in schema.fields().iter().enumerate() {
        if field.dict_id() == Some(id) {
            // Add (possibly multiple) array refs to the dictionaries array.
            dictionaries_by_field[i] = Some(dictionary_values.clone());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_iter() {
        let iter = 1..6;
        let iter = ProjectionIter::new(&[0, 2, 4], iter);
        let result: Vec<_> = iter.collect();
        use ProjectionResult::*;
        assert_eq!(
            result,
            vec![
                Selected(1),
                NotSelected(2),
                Selected(3),
                NotSelected(4),
                Selected(5)
            ]
        )
    }
}
