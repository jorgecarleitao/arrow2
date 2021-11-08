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

use arrow_format::ipc;
use arrow_format::ipc::Schema::MetadataVersion;

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
    dictionaries: &HashMap<usize, Arc<dyn Array>>,
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

    let mut field_nodes = field_nodes.iter().collect::<VecDeque<_>>();

    let (schema, columns) = if let Some(projection) = projection {
        let projected_schema = projection.1.clone();

        let projection = ProjectionIter::new(projection.0, schema.fields().iter());

        let arrays = projection
            .map(|maybe_field| match maybe_field {
                ProjectionResult::Selected(field) => Some(read(
                    &mut field_nodes,
                    field,
                    &mut buffers,
                    reader,
                    dictionaries,
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
                    field,
                    &mut buffers,
                    reader,
                    dictionaries,
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

fn find_first_dict_field_d(id: usize, data_type: &DataType) -> Option<&Field> {
    use DataType::*;
    match data_type {
        Dictionary(_, inner) => find_first_dict_field_d(id, inner.as_ref()),
        Map(field, _) => find_first_dict_field(id, field.as_ref()),
        List(field) => find_first_dict_field(id, field.as_ref()),
        LargeList(field) => find_first_dict_field(id, field.as_ref()),
        FixedSizeList(field, _) => find_first_dict_field(id, field.as_ref()),
        Union(fields, _, _) => {
            for field in fields {
                if let Some(f) = find_first_dict_field(id, field) {
                    return Some(f);
                }
            }
            None
        }
        Struct(fields) => {
            for field in fields {
                if let Some(f) = find_first_dict_field(id, field) {
                    return Some(f);
                }
            }
            None
        }
        _ => None,
    }
}

fn find_first_dict_field(id: usize, field: &Field) -> Option<&Field> {
    if let DataType::Dictionary(_, _) = &field.data_type {
        if field.dict_id as usize == id {
            return Some(field);
        }
    }
    find_first_dict_field_d(id, &field.data_type)
}

fn first_dict_field(id: usize, fields: &[Field]) -> Result<&Field> {
    for field in fields {
        if let Some(field) = find_first_dict_field(id, field) {
            return Ok(field);
        }
    }
    Err(ArrowError::Schema(format!(
        "dictionary id {} not found in schema",
        id
    )))
}

/// Read the dictionary from the buffer and provided metadata,
/// updating the `dictionaries` with the resulting dictionary
pub fn read_dictionary<R: Read + Seek>(
    batch: ipc::Message::DictionaryBatch,
    schema: &Schema,
    is_little_endian: bool,
    dictionaries: &mut HashMap<usize, Arc<dyn Array>>,
    reader: &mut R,
    block_offset: u64,
) -> Result<()> {
    if batch.isDelta() {
        return Err(ArrowError::NotYetImplemented(
            "delta dictionary batches not supported".to_string(),
        ));
    }

    let id = batch.id();
    let first_field = first_dict_field(id as usize, &schema.fields)?;

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
                dictionaries,
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

    dictionaries.insert(id as usize, dictionary_values);

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
