use std::{
    io::{Read, Seek},
    rc::Rc,
    sync::Arc,
};

use crate::{
    datatypes::{Field, Schema},
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};

use super::{
    get_page_iterator, get_schema, page_iter_to_array, read_metadata, Decompressor, FileMetaData,
    PageFilter, RowGroupMetaData,
};

type GroupFilter = Arc<dyn Fn(usize, &RowGroupMetaData) -> bool>;

/// Single threaded iterator of [`RecordBatch`] from a parquet file.
pub struct RecordReader<R: Read + Seek> {
    reader: R,
    schema: Arc<Schema>,
    indices: Rc<Vec<usize>>,
    buffer: Vec<u8>,
    decompress_buffer: Vec<u8>,
    groups_filter: GroupFilter,
    pages_filter: Option<PageFilter>,
    metadata: Rc<FileMetaData>,
    current_group: usize,
    remaining_rows: usize,
}

impl<R: Read + Seek> RecordReader<R> {
    pub fn try_new(
        mut reader: R,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        groups_filter: GroupFilter,
        pages_filter: Option<PageFilter>,
    ) -> Result<Self> {
        let metadata = read_metadata(&mut reader)?;

        let schema = get_schema(&metadata)?;

        let schema_metadata = schema.metadata;
        let (indices, fields): (Vec<usize>, Vec<Field>) = if let Some(projection) = &projection {
            schema
                .fields
                .into_iter()
                .enumerate()
                .filter_map(|(index, f)| {
                    if projection.iter().any(|&i| i == index) {
                        Some((index, f))
                    } else {
                        None
                    }
                })
                .unzip()
        } else {
            schema.fields.into_iter().enumerate().unzip()
        };

        if let Some(projection) = &projection {
            if indices.len() != projection.len() {
                return Err(ArrowError::InvalidArgumentError("While reading parquet, some columns in the projection do not exist in the file".to_string()));
            }
        }

        let schema = Arc::new(Schema {
            fields,
            metadata: schema_metadata,
        });

        Ok(Self {
            reader,
            schema,
            indices: Rc::new(indices),
            groups_filter,
            pages_filter,
            metadata: Rc::new(metadata),
            current_group: 0,
            buffer: vec![],
            decompress_buffer: vec![],
            remaining_rows: limit.unwrap_or(usize::MAX),
        })
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

impl<R: Read + Seek> Iterator for RecordReader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.schema.fields().is_empty() {
            return None;
        }
        if self.current_group == self.metadata.row_groups.len() {
            return None;
        };
        let remaining_rows = self.remaining_rows;
        if remaining_rows == 0 {
            return None;
        }

        let row_group = self.current_group;
        let metadata = self.metadata.clone();
        let group = &metadata.row_groups[row_group];
        if !(self.groups_filter)(row_group, group) {
            self.current_group += 1;
            return self.next();
        }
        let columns_meta = group.columns();

        // todo: avoid these clones.
        let schema = self.schema().clone();

        let b1 = std::mem::take(&mut self.buffer);
        let b2 = std::mem::take(&mut self.decompress_buffer);

        let a = schema.fields().iter().enumerate().try_fold(
            (b1, b2, Vec::with_capacity(schema.fields().len())),
            |(b1, b2, mut columns), (column, field)| {
                // column according to the file's indexing
                let column = self.indices[column];
                let column_meta = &columns_meta[column];
                let pages = get_page_iterator(
                    &metadata,
                    row_group,
                    column,
                    &mut self.reader,
                    self.pages_filter.clone(),
                    b1,
                )?;

                let mut pages = Decompressor::new(pages, b2);

                let array = page_iter_to_array(&mut pages, column_meta, field.data_type().clone())?;

                let array = if array.len() > remaining_rows {
                    array.slice(0, remaining_rows)
                } else {
                    array
                };

                columns.push(array.into());
                let (b1, b2) = pages.into_buffers();
                Result::Ok((b1, b2, columns))
            },
        );

        self.current_group += 1;
        Some(a.and_then(|(b1, b2, columns)| {
            self.buffer = b1;
            self.decompress_buffer = b2;
            RecordBatch::try_new(self.schema.clone(), columns).map(|batch| {
                self.remaining_rows -= batch.num_rows();
                batch
            })
        }))
    }
}
