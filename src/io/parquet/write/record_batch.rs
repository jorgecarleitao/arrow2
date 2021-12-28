use parquet2::write::Compressor;
use parquet2::FallibleStreamingIterator;

use super::{
    array_to_pages, to_parquet_schema, DynIter, DynStreamingIterator, Encoding, RowGroupIter,
    SchemaDescriptor, WriteOptions,
};
use crate::{
    array::Array,
    chunk::Chunk,
    datatypes::Schema,
    error::{ArrowError, Result},
};

/// An iterator adapter that converts an iterator over [`Chunk`] into an iterator
/// of row groups.
/// Use it to create an iterator consumable by the parquet's API.
pub struct RowGroupIterator<A: AsRef<dyn Array> + 'static, I: Iterator<Item = Result<Chunk<A>>>> {
    iter: I,
    options: WriteOptions,
    parquet_schema: SchemaDescriptor,
    encodings: Vec<Encoding>,
}

impl<A: AsRef<dyn Array> + 'static, I: Iterator<Item = Result<Chunk<A>>>> RowGroupIterator<A, I> {
    /// Creates a new [`RowGroupIterator`] from an iterator over [`Chunk`].
    pub fn try_new(
        iter: I,
        schema: &Schema,
        options: WriteOptions,
        encodings: Vec<Encoding>,
    ) -> Result<Self> {
        assert_eq!(schema.fields().len(), encodings.len());

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

impl<A: AsRef<dyn Array> + 'static, I: Iterator<Item = Result<Chunk<A>>>> Iterator
    for RowGroupIterator<A, I>
{
    type Item = Result<RowGroupIter<'static, ArrowError>>;

    fn next(&mut self) -> Option<Self::Item> {
        let options = self.options;

        self.iter.next().map(|maybe_chunk| {
            let columns = maybe_chunk?;
            let encodings = self.encodings.clone();
            Ok(DynIter::new(
                columns
                    .into_arrays()
                    .into_iter()
                    .zip(self.parquet_schema.columns().to_vec().into_iter())
                    .zip(encodings.into_iter())
                    .map(move |((array, descriptor), encoding)| {
                        array_to_pages(array.as_ref(), descriptor, options, encoding).map(
                            move |pages| {
                                let encoded_pages = DynIter::new(pages.map(|x| Ok(x?)));
                                let compressed_pages =
                                    Compressor::new(encoded_pages, options.compression, vec![])
                                        .map_err(ArrowError::from);
                                DynStreamingIterator::new(compressed_pages)
                            },
                        )
                    }),
            ))
        })
    }
}
