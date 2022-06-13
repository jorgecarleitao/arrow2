use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Seek};

use arrow_format;

use crate::array::*;
use crate::chunk::Chunk;
use crate::datatypes::{DataType, Field};
use crate::error::{Error, Result};
use crate::io::ipc::{IpcField, IpcSchema};

use super::deserialize::{read, skip};
use super::Dictionaries;

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
    batch: arrow_format::ipc::RecordBatchRef,
    fields: &[Field],
    ipc_schema: &IpcSchema,
    projection: Option<&[usize]>,
    dictionaries: &Dictionaries,
    version: arrow_format::ipc::MetadataVersion,
    reader: &mut R,
    block_offset: u64,
    file_size: u64,
) -> Result<Chunk<Box<dyn Array>>> {
    assert_eq!(fields.len(), ipc_schema.fields.len());
    let buffers = batch
        .buffers()?
        .ok_or_else(|| Error::oos("IPC RecordBatch must contain buffers"))?;
    let mut buffers: VecDeque<arrow_format::ipc::BufferRef> = buffers.iter().collect();

    for buffer in buffers.iter() {
        if buffer.length() as u64 > file_size {
            return Err(Error::oos(
                "Any buffer's length must be smaller than the size of the file",
            ));
        }
    }

    let field_nodes = batch
        .nodes()?
        .ok_or_else(|| Error::oos("IPC RecordBatch must contain field nodes"))?;
    let mut field_nodes = field_nodes.iter().collect::<VecDeque<_>>();

    let columns = if let Some(projection) = projection {
        let projection =
            ProjectionIter::new(projection, fields.iter().zip(ipc_schema.fields.iter()));

        projection
            .map(|maybe_field| match maybe_field {
                ProjectionResult::Selected((field, ipc_field)) => Ok(Some(read(
                    &mut field_nodes,
                    field,
                    ipc_field,
                    &mut buffers,
                    reader,
                    dictionaries,
                    block_offset,
                    ipc_schema.is_little_endian,
                    batch.compression()?,
                    version,
                )?)),
                ProjectionResult::NotSelected((field, _)) => {
                    skip(&mut field_nodes, &field.data_type, &mut buffers)?;
                    Ok(None)
                }
            })
            .filter_map(|x| x.transpose())
            .collect::<Result<Vec<_>>>()?
    } else {
        fields
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
                    batch.compression()?,
                    version,
                )
            })
            .collect::<Result<Vec<_>>>()?
    };
    Chunk::try_new(columns)
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
    Err(Error::OutOfSpec(format!(
        "dictionary id {} not found in schema",
        id
    )))
}

/// Read the dictionary from the buffer and provided metadata,
/// updating the `dictionaries` with the resulting dictionary
pub fn read_dictionary<R: Read + Seek>(
    batch: arrow_format::ipc::DictionaryBatchRef,
    fields: &[Field],
    ipc_schema: &IpcSchema,
    dictionaries: &mut Dictionaries,
    reader: &mut R,
    block_offset: u64,
    file_size: u64,
) -> Result<()> {
    if batch.is_delta()? {
        return Err(Error::NotYetImplemented(
            "delta dictionary batches not supported".to_string(),
        ));
    }

    let id = batch.id()?;
    let (first_field, first_ipc_field) = first_dict_field(id, fields, &ipc_schema.fields)?;

    // As the dictionary batch does not contain the type of the
    // values array, we need to retrieve this from the schema.
    // Get an array representing this dictionary's values.
    let dictionary_values: Box<dyn Array> = match &first_field.data_type {
        DataType::Dictionary(_, ref value_type, _) => {
            let batch = batch
                .data()?
                .ok_or_else(|| Error::oos("The dictionary batch must have data."))?;

            // Make a fake schema for the dictionary batch.
            let fields = vec![Field::new("", value_type.as_ref().clone(), false)];
            let ipc_schema = IpcSchema {
                fields: vec![first_ipc_field.clone()],
                is_little_endian: ipc_schema.is_little_endian,
            };
            let columns = read_record_batch(
                batch,
                &fields,
                &ipc_schema,
                None,
                dictionaries,
                arrow_format::ipc::MetadataVersion::V5,
                reader,
                block_offset,
                file_size,
            )?;
            let mut arrays = columns.into_arrays();
            Some(arrays.pop().unwrap())
        }
        _ => None,
    }
    .ok_or_else(|| Error::InvalidArgumentError("dictionary id not found in schema".to_string()))?;

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

pub fn prepare_projection(
    fields: &[Field],
    mut projection: Vec<usize>,
) -> (Vec<usize>, HashMap<usize, usize>, Vec<Field>) {
    assert_eq!(
        projection.iter().collect::<HashSet<_>>().len(),
        projection.len(),
        "The projection on IPC must not contain duplicates"
    );

    let fields = projection.iter().map(|x| fields[*x].clone()).collect();

    // selected index; index in
    let sorted_projection = projection
        .iter()
        .copied()
        .enumerate()
        .map(|x| (x.1, x.0))
        .collect::<HashMap<_, _>>(); // e.g. [2, 1] -> {2: 0, 1: 1}
    projection.sort_unstable(); // e.g. [2, 1] -> [1, 2]

    (projection, sorted_projection, fields)
}

pub fn apply_projection(
    chunk: Chunk<Box<dyn Array>>,
    projection: &[usize],
    map: &HashMap<usize, usize>,
) -> Chunk<Box<dyn Array>> {
    // re-order according to projection
    let arrays = chunk.into_arrays();
    let arrays = projection
        .iter()
        .map(|x| {
            let index = map.get(x).unwrap();
            arrays[*index].clone()
        })
        .collect();
    Chunk::new(arrays)
}
