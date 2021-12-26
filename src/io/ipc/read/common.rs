use std::collections::{HashMap, VecDeque};
use std::io::{Read, Seek};
use std::sync::Arc;

use arrow_format::ipc;
use arrow_format::ipc::Schema::MetadataVersion;

use crate::array::*;
use crate::datatypes::{DataType, Field, Schema};
use crate::error::{ArrowError, Result};
use crate::io::ipc::{IpcField, IpcSchema};
use crate::record_batch::RecordBatch;

use super::deserialize::{read, skip};
use super::Dictionaries;

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
    ipc_schema: &IpcSchema,
    projection: Option<(&[usize], Arc<Schema>)>,
    dictionaries: &Dictionaries,
    version: MetadataVersion,
    reader: &mut R,
    block_offset: u64,
) -> Result<RecordBatch> {
    assert_eq!(schema.fields().len(), ipc_schema.fields.len());
    let buffers = batch.buffers().ok_or_else(|| {
        ArrowError::OutOfSpec("Unable to get buffers from IPC RecordBatch".to_string())
    })?;
    let mut buffers: VecDeque<&ipc::Schema::Buffer> = buffers.iter().collect();
    let field_nodes = batch.nodes().ok_or_else(|| {
        ArrowError::OutOfSpec("Unable to get field nodes from IPC RecordBatch".to_string())
    })?;

    let mut field_nodes = field_nodes.iter().collect::<VecDeque<_>>();

    let (schema, columns) = if let Some(projection) = projection {
        let projected_schema = projection.1.clone();

        let projection = ProjectionIter::new(
            projection.0,
            schema.fields().iter().zip(ipc_schema.fields.iter()),
        );

        let arrays = projection
            .map(|maybe_field| match maybe_field {
                ProjectionResult::Selected((field, ipc_field)) => Some(read(
                    &mut field_nodes,
                    field,
                    ipc_field,
                    &mut buffers,
                    reader,
                    dictionaries,
                    block_offset,
                    ipc_schema.is_little_endian,
                    batch.compression(),
                    version,
                )),
                ProjectionResult::NotSelected((field, _)) => {
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
            .zip(ipc_schema.fields.iter())
            .map(|(field, ipc_field)| {
                read(
                    &mut field_nodes,
                    field,
                    ipc_field,
                    &mut buffers,
                    reader,
                    dictionaries,
                    block_offset,
                    ipc_schema.is_little_endian,
                    batch.compression(),
                    version,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        (schema.clone(), arrays)
    };
    RecordBatch::try_new(schema, columns)
}

fn find_first_dict_field_d<'a>(
    id: i64,
    data_type: &'a DataType,
    ipc_field: &'a IpcField,
) -> Option<(&'a Field, &'a IpcField)> {
    use DataType::*;
    match data_type {
        Dictionary(_, inner, _) => find_first_dict_field_d(id, inner.as_ref(), ipc_field),
        List(field) | LargeList(field) | FixedSizeList(field, ..) | Map(field, ..) => {
            find_first_dict_field(id, field.as_ref(), &ipc_field.fields[0])
        }
        Union(fields, ..) | Struct(fields) => {
            for (field, ipc_field) in fields.iter().zip(ipc_field.fields.iter()) {
                if let Some(f) = find_first_dict_field(id, field, ipc_field) {
                    return Some(f);
                }
            }
            None
        }
        _ => None,
    }
}

fn find_first_dict_field<'a>(
    id: i64,
    field: &'a Field,
    ipc_field: &'a IpcField,
) -> Option<(&'a Field, &'a IpcField)> {
    if let Some(field_id) = ipc_field.dictionary_id {
        if id == field_id {
            return Some((field, ipc_field));
        }
    }
    find_first_dict_field_d(id, &field.data_type, ipc_field)
}

fn first_dict_field<'a>(
    id: i64,
    fields: &'a [Field],
    ipc_fields: &'a [IpcField],
) -> Result<(&'a Field, &'a IpcField)> {
    assert_eq!(fields.len(), ipc_fields.len());
    for (field, ipc_field) in fields.iter().zip(ipc_fields.iter()) {
        if let Some(field) = find_first_dict_field(id, field, ipc_field) {
            return Ok(field);
        }
    }
    Err(ArrowError::OutOfSpec(format!(
        "dictionary id {} not found in schema",
        id
    )))
}

/// Read the dictionary from the buffer and provided metadata,
/// updating the `dictionaries` with the resulting dictionary
pub fn read_dictionary<R: Read + Seek>(
    batch: ipc::Message::DictionaryBatch,
    fields: &[Field],
    ipc_schema: &IpcSchema,
    dictionaries: &mut Dictionaries,
    reader: &mut R,
    block_offset: u64,
) -> Result<()> {
    if batch.isDelta() {
        return Err(ArrowError::NotYetImplemented(
            "delta dictionary batches not supported".to_string(),
        ));
    }

    let id = batch.id();
    let (first_field, first_ipc_field) = first_dict_field(id, fields, &ipc_schema.fields)?;

    // As the dictionary batch does not contain the type of the
    // values array, we need to retrieve this from the schema.
    // Get an array representing this dictionary's values.
    let dictionary_values: ArrayRef = match first_field.data_type() {
        DataType::Dictionary(_, ref value_type, _) => {
            // Make a fake schema for the dictionary batch.
            let schema = Arc::new(Schema {
                fields: vec![Field::new("", value_type.as_ref().clone(), false)],
                metadata: HashMap::new(),
            });
            let ipc_schema = IpcSchema {
                fields: vec![first_ipc_field.clone()],
                is_little_endian: ipc_schema.is_little_endian,
            };
            assert_eq!(ipc_schema.fields.len(), schema.fields().len());
            // Read a single column
            let record_batch = read_record_batch(
                batch.data().unwrap(),
                schema,
                &ipc_schema,
                None,
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

    dictionaries.insert(id, dictionary_values);

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
