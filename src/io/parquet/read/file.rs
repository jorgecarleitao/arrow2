use std::io::{Read, Seek};
use std::sync::Arc;

use crate::array::Array;
use crate::chunk::Chunk;
use crate::datatypes::Schema;
use crate::io::parquet::read::read_columns_many;
use crate::{
    datatypes::Field,
    error::{Error, Result},
};

use super::{infer_schema, FileMetaData, RowGroupDeserializer, RowGroupMetaData};

type GroupFilter = Arc<dyn Fn(usize, &RowGroupMetaData) -> bool + Send + Sync>;

/// An iterator of [`Chunk`]s coming from row groups of a parquet file.
///
/// This can be thought of a flatten chain of [`Iterator<Item=Chunk>`] - each row group is sequentially
/// mapped to an [`Iterator<Item=Chunk>`] and each iterator is iterated upon until either the limit
/// or the last iterator ends.
/// # Implementation
/// This iterator mixes IO-bounded and CPU-bounded operations.
pub struct FileReader<R: Read + Seek> {
    row_groups: RowGroupReader<R>,
    metadata: FileMetaData,
    remaining_rows: usize,
    current_row_group: Option<RowGroupDeserializer>,
}

impl<R: Read + Seek> FileReader<R> {
    /// Returns a new [`FileReader`].
    ///
    /// # Error
    /// This function errors iff:
    /// * it is not possible to derive an arrow schema from the parquet file
    /// * the projection contains columns that do not exist
    pub fn try_new(
        reader: R,
        metadata: FileMetaData,
        projection: Option<&[usize]>,
        chunk_size: Option<usize>,
        limit: Option<usize>,
        groups_filter: Option<GroupFilter>,
    ) -> Result<Self> {
        let schema = infer_schema(&metadata)?;

        let schema_metadata = schema.metadata;
        let fields: Vec<Field> = if let Some(projection) = &projection {
            schema
                .fields
                .into_iter()
                .enumerate()
                .filter_map(|(index, f)| {
                    if projection.iter().any(|&i| i == index) {
                        Some(f)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            schema.fields.into_iter().collect()
        };

        if let Some(projection) = &projection {
            if fields.len() != projection.len() {
                return Err(Error::InvalidArgumentError(
                    "While reading parquet, some columns in the projection do not exist in the file"
                        .to_string(),
                ));
            }
        }

        let schema = Schema {
            fields,
            metadata: schema_metadata,
        };

        let row_groups = RowGroupReader::new(
            reader,
            schema,
            groups_filter,
            metadata.row_groups.clone(),
            chunk_size,
            limit,
        );

        Ok(Self {
            row_groups,
            metadata,
            remaining_rows: limit.unwrap_or(usize::MAX),
            current_row_group: None,
        })
    }

    /// Returns the derived arrow [`Schema`] of the file
    pub fn schema(&self) -> &Schema {
        &self.row_groups.schema
    }

    /// Returns parquet's [`FileMetaData`].
    pub fn metadata(&self) -> &FileMetaData {
        &self.metadata
    }

    /// Sets the groups filter
    pub fn set_groups_filter(&mut self, groups_filter: GroupFilter) {
        self.row_groups.set_groups_filter(groups_filter);
    }

    fn next_row_group(&mut self) -> Result<Option<RowGroupDeserializer>> {
        let result = self.row_groups.next().transpose()?;

        // If current_row_group is None, then there will be no elements to remove.
        if self.current_row_group.is_some() {
            self.remaining_rows = self.remaining_rows.saturating_sub(
                result
                    .as_ref()
                    .map(|x| x.num_rows())
                    .unwrap_or(self.remaining_rows),
            );
        }
        Ok(result)
    }
}

impl<R: Read + Seek> Iterator for FileReader<R> {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_rows == 0 {
            // reached the limit
            return None;
        }

        if let Some(row_group) = &mut self.current_row_group {
            match row_group.next() {
                // no more chunks in the current row group => try a new one
                None => match self.next_row_group() {
                    Ok(Some(row_group)) => {
                        self.current_row_group = Some(row_group);
                        // new found => pull again
                        self.next()
                    }
                    Ok(None) => {
                        self.current_row_group = None;
                        None
                    }
                    Err(e) => Some(Err(e)),
                },
                other => other,
            }
        } else {
            match self.next_row_group() {
                Ok(Some(row_group)) => {
                    self.current_row_group = Some(row_group);
                    self.next()
                }
                Ok(None) => {
                    self.current_row_group = None;
                    None
                }
                Err(e) => Some(Err(e)),
            }
        }
    }
}

/// An [`Iterator<Item=RowGroupDeserializer>`] from row groups of a parquet file.
///
/// # Implementation
/// Advancing this iterator is IO-bounded - each iteration reads all the column chunks from the file
/// to memory and attaches [`RowGroupDeserializer`] to them so that they can be iterated in chunks.
pub struct RowGroupReader<R: Read + Seek> {
    reader: R,
    schema: Schema,
    groups_filter: Option<GroupFilter>,
    row_groups: std::iter::Enumerate<std::vec::IntoIter<RowGroupMetaData>>,
    chunk_size: Option<usize>,
    remaining_rows: usize,
}

impl<R: Read + Seek> RowGroupReader<R> {
    /// Returns a new [`RowGroupReader`]
    pub fn new(
        reader: R,
        schema: Schema,
        groups_filter: Option<GroupFilter>,
        row_groups: Vec<RowGroupMetaData>,
        chunk_size: Option<usize>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            reader,
            schema,
            groups_filter,
            row_groups: row_groups.into_iter().enumerate(),
            chunk_size,
            remaining_rows: limit.unwrap_or(usize::MAX),
        }
    }

    /// Sets the groups filter
    pub fn set_groups_filter(&mut self, groups_filter: GroupFilter) {
        self.groups_filter = Some(groups_filter);
    }

    #[inline]
    fn _next(&mut self) -> Result<Option<RowGroupDeserializer>> {
        if self.schema.fields.is_empty() {
            return Ok(None);
        }
        if self.remaining_rows == 0 {
            // reached the limit
            return Ok(None);
        }

        let row_group = if let Some(groups_filter) = self.groups_filter.as_ref() {
            self.row_groups
                .by_ref()
                .find(|(index, row_group)| (groups_filter)(*index, row_group))
        } else {
            self.row_groups.next()
        };
        let row_group = if let Some((_, row_group)) = row_group {
            row_group
        } else {
            return Ok(None);
        };

        let column_chunks = read_columns_many(
            &mut self.reader,
            &row_group,
            self.schema.fields.clone(),
            self.chunk_size,
            Some(self.remaining_rows),
        )?;

        let result = RowGroupDeserializer::new(
            column_chunks,
            row_group.num_rows() as usize,
            Some(self.remaining_rows),
        );
        self.remaining_rows = self
            .remaining_rows
            .saturating_sub(row_group.num_rows() as usize);
        Ok(Some(result))
    }
}

impl<R: Read + Seek> Iterator for RowGroupReader<R> {
    type Item = Result<RowGroupDeserializer>;

    fn next(&mut self) -> Option<Self::Item> {
        self._next().transpose()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.row_groups.size_hint()
    }
}
