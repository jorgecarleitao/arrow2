use std::{
    io::{Read, Seek},
    rc::Rc,
    sync::Arc,
};

use crate::{datatypes::Schema, error::Result, record_batch::RecordBatch};

use super::{
    get_page_iterator, get_schema, page_iter_to_array, read_metadata, Decompressor, FileMetaData,
    RowGroupMetaData,
};

type GroupFilter = Arc<dyn Fn(usize, &RowGroupMetaData) -> bool>;

/// Single threaded iterator of [`RecordBatch`] from a parquet file.
pub struct RecordReader<R: Read + Seek> {
    reader: R,
    schema: Arc<Schema>,
    projection: Rc<Vec<usize>>,
    buffer: Vec<u8>,
    decompress_buffer: Vec<u8>,
    groups_filter: GroupFilter,
    metadata: Rc<FileMetaData>,
    current_group: usize,
    remaining_rows: usize,
}

impl<R: Read + Seek> RecordReader<R> {
    pub fn try_new(
        mut reader: R,
        schema: Option<Arc<Schema>>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        groups_filter: GroupFilter,
    ) -> Result<Self> {
        let metadata = read_metadata(&mut reader)?;

        let schema = schema
            .map(Ok)
            .unwrap_or_else(|| get_schema(&metadata).map(Arc::new))?;

        let projection = projection.unwrap_or_else(|| (0..schema.fields().len()).collect());
        Ok(Self {
            reader,
            schema,
            projection: Rc::new(projection),
            groups_filter,
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
        let projection = self.projection.clone();

        let b1 = std::mem::take(&mut self.buffer);
        let b2 = std::mem::take(&mut self.decompress_buffer);

        let a = self.schema.clone().fields().iter().enumerate().try_fold(
            (b1, b2, Vec::with_capacity(self.schema.fields().len())),
            |(b1, b2, mut columns), (column, field)| {
                let column = projection[column];
                let column_meta = &columns_meta[column];
                let pages = get_page_iterator(&metadata, row_group, column, &mut self.reader, b1)?;
                let mut pages = Decompressor::new(pages, b2);

                let array = page_iter_to_array(&mut pages, column_meta)?;

                let array = if array.len() > remaining_rows {
                    array.slice(0, remaining_rows)
                } else {
                    array
                };

                let c = crate::compute::cast::cast(array.as_ref(), field.data_type())
                    .map(|x| x.into())?;
                columns.push(c);
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
