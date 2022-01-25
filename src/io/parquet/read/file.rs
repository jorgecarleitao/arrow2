use std::io::{Read, Seek};
use std::sync::Arc;

use parquet2::schema::types::ParquetType;

use crate::array::Array;
use crate::chunk::Chunk;
use crate::datatypes::Schema;
use crate::io::parquet::read::get_iterators;
use crate::{
    datatypes::Field,
    error::{ArrowError, Result},
};

use super::{get_schema, read_metadata, FileMetaData, RowGroupMetaData, RowGroupReader};

type GroupFilter = Arc<dyn Fn(usize, &RowGroupMetaData) -> bool>;

/// Single threaded iterator row groups of a paquet file.
pub struct FileReader<R: Read + Seek> {
    reader: R,
    schema: Arc<Schema>,
    parquet_fields: Vec<ParquetType>,
    groups_filter: Option<GroupFilter>,
    metadata: FileMetaData,
    current_group: usize,
    chunk_size: Option<usize>,
    remaining_rows: usize,
    current_row_group: Option<RowGroupReader>,
}

impl<R: Read + Seek> FileReader<R> {
    /// Creates a new [`FileReader`] by reading the metadata from `reader` and constructing
    /// Arrow's schema from it.
    pub fn try_new(
        mut reader: R,
        projection: Option<&[usize]>,
        chunk_size: Option<usize>,
        limit: Option<usize>,
        groups_filter: Option<GroupFilter>,
    ) -> Result<Self> {
        let metadata = read_metadata(&mut reader)?;

        let schema = get_schema(&metadata)?;

        let schema_metadata = schema.metadata;
        let (fields, parquet_fields): (Vec<Field>, Vec<ParquetType>) =
            if let Some(projection) = &projection {
                schema
                    .fields
                    .into_iter()
                    .zip(metadata.schema().fields().iter().cloned())
                    .enumerate()
                    .filter_map(|(index, f)| {
                        if projection.iter().any(|&i| i == index) {
                            Some(f)
                        } else {
                            None
                        }
                    })
                    .unzip()
            } else {
                schema
                    .fields
                    .into_iter()
                    .zip(metadata.schema().fields().iter().cloned())
                    .unzip()
            };

        if let Some(projection) = &projection {
            if fields.len() != projection.len() {
                return Err(ArrowError::InvalidArgumentError(
                    "While reading parquet, some columns in the projection do not exist in the file"
                        .to_string(),
                ));
            }
        }

        let schema = Arc::new(Schema {
            fields,
            metadata: schema_metadata,
        });

        Ok(Self {
            reader,
            schema,
            parquet_fields,
            groups_filter,
            metadata,
            current_group: 0,
            chunk_size,
            remaining_rows: limit.unwrap_or(usize::MAX),
            current_row_group: None,
        })
    }

    /// Returns the [`Schema`]
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Returns parquet's [`FileMetaData`].
    pub fn metadata(&self) -> &FileMetaData {
        &self.metadata
    }

    /// Sets the groups filter
    pub fn set_groups_filter(&mut self, groups_filter: GroupFilter) {
        self.groups_filter = Some(groups_filter);
    }

    fn next_row_group(&mut self) -> Result<Option<RowGroupReader>> {
        if self.schema.fields.is_empty() {
            return Ok(None);
        }
        if self.current_group == self.metadata.row_groups.len() {
            // reached the last row group
            return Ok(None);
        };
        if self.remaining_rows == 0 {
            // reached the limit
            return Ok(None);
        }

        let current_row_group = self.current_group;
        let row_group = &self.metadata.row_groups[current_row_group];
        if let Some(groups_filter) = self.groups_filter.as_ref() {
            if !(groups_filter)(current_row_group, row_group) {
                self.current_group += 1;
                return self.next_row_group();
            }
        }
        self.current_group += 1;

        let column_chunks = get_iterators(
            &mut self.reader,
            &self.parquet_fields,
            row_group,
            self.schema.fields.clone(),
            self.chunk_size,
        )?;

        let result = RowGroupReader::new(
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

impl<R: Read + Seek> Iterator for FileReader<R> {
    type Item = Result<Chunk<Arc<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.schema.fields.is_empty() {
            return None;
        }
        if self.remaining_rows == 0 {
            // reached the limit
            return None;
        }

        if let Some(row_group) = &mut self.current_row_group {
            match row_group.next() {
                None => match self.next_row_group() {
                    Ok(Some(row_group)) => {
                        self.current_row_group = Some(row_group);
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
