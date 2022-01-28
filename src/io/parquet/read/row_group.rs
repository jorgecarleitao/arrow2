use std::{
    io::{Read, Seek},
    sync::Arc,
};

use parquet2::{
    metadata::ColumnChunkMetaData,
    read::{BasicDecompressor, PageIterator},
    schema::types::ParquetType,
};

use crate::{
    array::Array, chunk::Chunk, datatypes::Field, error::Result,
    io::parquet::read::page_iter_to_arrays,
};

use super::RowGroupMetaData;

pub struct RowGroupReader {
    remaining_rows: usize,
    column_chunks: Vec<Box<dyn Iterator<Item = Result<Arc<dyn Array>>>>>,
}

fn get_field_columns<'a>(
    row_group: &'a RowGroupMetaData,
    field: &ParquetType,
) -> Vec<&'a ColumnChunkMetaData> {
    row_group
        .columns()
        .iter()
        .enumerate()
        .filter(|x| x.1.descriptor().path_in_schema()[0] == field.name())
        .map(|x| x.1)
        .collect()
}

pub(super) fn get_iterators<R: Read + Seek>(
    reader: &mut R,
    parquet_fields: &[ParquetType],
    row_group: &RowGroupMetaData,
    fields: Vec<Field>,
    chunk_size: Option<usize>,
) -> Result<Vec<Box<dyn Iterator<Item = Result<Arc<dyn Array>>>>>> {
    // reads all the necessary columns for all fields from the row group
    // This operation is IO-bounded `O(C)` where C is the number of columns in the row group
    fields
        .iter()
        .zip(parquet_fields.iter())
        .map(|(field, parquet_field)| {
            let chunks = get_field_columns(row_group, parquet_field)
                .into_iter()
                .map(|meta| {
                    let (start, len) = meta.byte_range();
                    reader.seek(std::io::SeekFrom::Start(start))?;
                    let mut chunk = vec![0; len as usize];
                    reader.read_exact(&mut chunk)?;
                    Ok((meta, chunk))
                });

            chunks
                .map(|x| {
                    x.and_then(|(column_meta, chunk)| {
                        let pages = PageIterator::new(
                            std::io::Cursor::new(chunk),
                            column_meta.num_values(),
                            column_meta.compression(),
                            column_meta.descriptor().clone(),
                            Arc::new(|_, _| true),
                            vec![],
                        );
                        let pages = BasicDecompressor::new(pages, vec![]);
                        page_iter_to_arrays(
                            pages,
                            column_meta,
                            field.clone(),
                            chunk_size
                                .unwrap_or(usize::MAX)
                                .min(row_group.num_rows() as usize),
                        )
                    })
                })
                // todo: generalize for struct type
                .next()
                .unwrap()
        })
        .collect()
}

impl RowGroupReader {
    pub fn new(
        column_chunks: Vec<Box<dyn Iterator<Item = Result<Arc<dyn Array>>>>>,
        num_rows: usize,
        limit: Option<usize>,
    ) -> Self {
        Self {
            remaining_rows: limit.unwrap_or(usize::MAX).min(num_rows),
            column_chunks,
        }
    }
}

impl Iterator for RowGroupReader {
    type Item = Result<Chunk<Arc<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_rows == 0 {
            return None;
        }
        let chunk = self
            .column_chunks
            .iter_mut()
            .map(|iter| {
                let array = iter.next().unwrap()?;
                Ok(if array.len() > self.remaining_rows {
                    array.slice(0, array.len() - self.remaining_rows).into()
                } else {
                    array
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(Chunk::new);
        self.remaining_rows -= chunk
            .as_ref()
            .map(|x| x.len())
            .unwrap_or(self.remaining_rows);

        Some(chunk)
    }
}
