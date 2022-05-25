use parquet2::error::Error as ParquetError;
use parquet2::schema::types::ParquetType;
use parquet2::write::Compressor;
use parquet2::FallibleStreamingIterator;

use crate::{
    array::Array,
    chunk::Chunk,
    datatypes::Schema,
    error::{ArrowError, Result},
};

use super::{
    array_to_columns, to_parquet_schema, DynIter, DynStreamingIterator, Encoding, RowGroupIter,
    SchemaDescriptor, WriteOptions,
};

/// Maps a [`Chunk`] and parquet-specific options to an [`RowGroupIter`] used to
/// write to parquet
pub fn row_group_iter<A: AsRef<dyn Array> + 'static + Send + Sync>(
    chunk: Chunk<A>,
    encodings: Vec<Vec<Encoding>>,
    fields: Vec<ParquetType>,
    options: WriteOptions,
) -> RowGroupIter<'static, ArrowError> {
    DynIter::new(
        chunk
            .into_arrays()
            .into_iter()
            .zip(fields.into_iter())
            .zip(encodings.into_iter())
            .flat_map(move |((array, type_), encoding)| {
                let encoded_columns = array_to_columns(array, type_, options, encoding).unwrap();
                encoded_columns
                    .into_iter()
                    .map(|encoded_pages| {
                        let pages = encoded_pages;

                        let pages = DynIter::new(
                            pages
                                .into_iter()
                                .map(|x| x.map_err(|e| ParquetError::General(e.to_string()))),
                        );

                        let compressed_pages = Compressor::new(pages, options.compression, vec![])
                            .map_err(ArrowError::from);
                        Ok(DynStreamingIterator::new(compressed_pages))
                    })
                    .collect::<Vec<_>>()
            }),
    )
}

/// An iterator adapter that converts an iterator over [`Chunk`] into an iterator
/// of row groups.
/// Use it to create an iterator consumable by the parquet's API.
pub struct RowGroupIterator<A: AsRef<dyn Array> + 'static, I: Iterator<Item = Result<Chunk<A>>>> {
    iter: I,
    options: WriteOptions,
    parquet_schema: SchemaDescriptor,
    encodings: Vec<Vec<Encoding>>,
}

impl<A: AsRef<dyn Array> + 'static, I: Iterator<Item = Result<Chunk<A>>>> RowGroupIterator<A, I> {
    /// Creates a new [`RowGroupIterator`] from an iterator over [`Chunk`].
    pub fn try_new(
        iter: I,
        schema: &Schema,
        options: WriteOptions,
        encodings: Vec<Vec<Encoding>>,
    ) -> Result<Self> {
        let parquet_schema = to_parquet_schema(schema)?;

        Ok(Self {
            iter,
            options,
            parquet_schema,
            encodings,
        })
    }

    /// Returns the [`SchemaDescriptor`] of the [`RowGroupIterator`].
    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        &self.parquet_schema
    }
}

impl<A: AsRef<dyn Array> + 'static + Send + Sync, I: Iterator<Item = Result<Chunk<A>>>> Iterator
    for RowGroupIterator<A, I>
{
    type Item = Result<RowGroupIter<'static, ArrowError>>;

    fn next(&mut self) -> Option<Self::Item> {
        let options = self.options;

        self.iter.next().map(|maybe_chunk| {
            let chunk = maybe_chunk?;
            let encodings = self.encodings.clone();
            Ok(row_group_iter(
                chunk,
                encodings,
                self.parquet_schema.fields().to_vec(),
                options,
            ))
        })
    }
}
