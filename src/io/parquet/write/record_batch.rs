use super::{
    array_to_page, to_parquet_schema, DynIter, RowGroupIter, SchemaDescriptor, WriteOptions,
};
use crate::{
    datatypes::Schema,
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};

/// An iterator adapter that converts an iterator over [`RecordBatch`] into an iterator
/// of row groups.
/// Use it to create an iterator consumable by the parquet's API.
pub struct RowGroupIterator<I: Iterator<Item = Result<RecordBatch>>> {
    iter: I,
    options: WriteOptions,
    parquet_schema: SchemaDescriptor,
}

impl<I: Iterator<Item = Result<RecordBatch>>> RowGroupIterator<I> {
    /// Creates a new [`RowGroupIterator`] from an iterator over [`RecordBatch`].
    pub fn try_new(iter: I, schema: &Schema, options: WriteOptions) -> Result<Self> {
        let parquet_schema = to_parquet_schema(schema)?;

        Ok(Self {
            iter,
            options,
            parquet_schema,
        })
    }

    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        &self.parquet_schema
    }
}

impl<I: Iterator<Item = Result<RecordBatch>>> Iterator for RowGroupIterator<I> {
    type Item = Result<RowGroupIter<'static, ArrowError>>;

    fn next(&mut self) -> Option<Self::Item> {
        let options = self.options;

        self.iter.next().map(|batch| {
            let columns = batch?.columns().to_vec();
            Ok(DynIter::new(
                columns
                    .into_iter()
                    .zip(self.parquet_schema.columns().to_vec().into_iter())
                    .map(move |(array, type_)| {
                        Ok(DynIter::new(std::iter::once(array_to_page(
                            array.as_ref(),
                            type_,
                            options,
                        ))))
                    }),
            ))
        })
    }
}
