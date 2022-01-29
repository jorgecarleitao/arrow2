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
    io::parquet::read::column_iter_to_arrays,
};

use super::RowGroupMetaData;

pub struct RowGroupReader {
    remaining_rows: usize,
    column_chunks: Vec<Box<dyn Iterator<Item = Result<Arc<dyn Array>>>>>,
}

fn get_field_columns<'a>(
    row_group: &'a RowGroupMetaData,
    field_name: &str,
) -> Vec<&'a ColumnChunkMetaData> {
    row_group
        .columns()
        .iter()
        .enumerate()
        .filter(|x| x.1.descriptor().path_in_schema()[0] == field_name)
        .map(|x| x.1)
        .collect()
}

/// Reads all columns that are part of the parquet field `field_name`
pub fn read_columns<'a, R: Read + Seek>(
    reader: &mut R,
    row_group: &'a RowGroupMetaData,
    field_name: &str,
) -> Result<Vec<(&'a ColumnChunkMetaData, Vec<u8>)>> {
    get_field_columns(row_group, field_name)
        .into_iter()
        .map(|meta| {
            let (start, len) = meta.byte_range();
            reader.seek(std::io::SeekFrom::Start(start))?;
            let mut chunk = vec![0; len as usize];
            reader.read_exact(&mut chunk)?;
            Ok((meta, chunk))
        })
        .collect()
}

pub(super) fn get_iterators<'a, R: Read + Seek>(
    reader: &mut R,
    parquet_fields: &[ParquetType],
    row_group: &RowGroupMetaData,
    fields: Vec<Field>,
    chunk_size: Option<usize>,
) -> Result<Vec<Box<dyn Iterator<Item = Result<Arc<dyn Array>>> + 'a>>> {
    let chunk_size = chunk_size
        .unwrap_or(usize::MAX)
        .min(row_group.num_rows() as usize);

    // reads all the necessary columns for all fields from the row group
    // This operation is IO-bounded `O(C)` where C is the number of columns in the row group
    let columns = parquet_fields
        .iter()
        .map(|parquet_field| read_columns(reader, row_group, parquet_field.name()))
        .collect::<Result<Vec<_>>>()?;

    columns
        .into_iter()
        .map(|columns| {
            let (pages, types): (Vec<_>, Vec<_>) = columns
                .into_iter()
                .map(|(column_meta, chunk)| {
                    let pages = PageIterator::new(
                        std::io::Cursor::new(chunk),
                        column_meta.num_values(),
                        column_meta.compression(),
                        column_meta.descriptor().clone(),
                        Arc::new(|_, _| true),
                        vec![],
                    );
                    (
                        BasicDecompressor::new(pages, vec![]),
                        column_meta.descriptor().type_(),
                    )
                })
                .unzip();
            (pages, types)
        })
        .zip(fields.into_iter())
        .map(|((columns, types), field)| column_iter_to_arrays(columns, types, &field, chunk_size))
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
