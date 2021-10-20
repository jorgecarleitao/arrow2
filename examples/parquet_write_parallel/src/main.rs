//! Example demonstrating how to write to parquet in parallel.
use std::collections::VecDeque;
use std::sync::Arc;

use rayon::prelude::*;

use arrow2::{
    array::*,
    datatypes::PhysicalType,
    error::{ArrowError, Result},
    io::parquet::write::*,
    record_batch::RecordBatch,
};

struct Bla {
    columns: VecDeque<CompressedPage>,
    current: Option<CompressedPage>,
}

impl Bla {
    pub fn new(columns: VecDeque<CompressedPage>) -> Self {
        Self {
            columns,
            current: None,
        }
    }
}

impl FallibleStreamingIterator for Bla {
    type Item = CompressedPage;
    type Error = ArrowError;

    fn advance(&mut self) -> Result<()> {
        self.current = self.columns.pop_front();
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}

fn parallel_write(path: &str, batches: &[RecordBatch]) -> Result<()> {
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Snappy,
        version: Version::V2,
    };
    let encodings = batches[0].schema().fields().par_iter().map(|field| {
        match field.data_type().to_physical_type() {
            // let's be fancy and use delta-encoding for binary fields
            PhysicalType::Binary
            | PhysicalType::LargeBinary
            | PhysicalType::Utf8
            | PhysicalType::LargeUtf8 => Encoding::DeltaLengthByteArray,
            // remaining is plain
            _ => Encoding::Plain,
        }
    });

    let parquet_schema = to_parquet_schema(batches[0].schema())?;

    let a = parquet_schema.clone();
    let row_groups = batches.iter().map(|batch| {
        // write batch to pages; parallelized by rayon
        let columns = batch
            .columns()
            .par_iter()
            .zip(a.columns().to_vec().into_par_iter())
            .zip(encodings.clone())
            .map(|((array, descriptor), encoding)| {
                // create encoded and compressed pages this column
                let encoded_pages = array_to_pages(array.as_ref(), descriptor, options, encoding)?;
                encoded_pages
                    .map(|page| compress(page?, vec![], options.compression).map_err(|x| x.into()))
                    .collect::<Result<VecDeque<_>>>()
            })
            .collect::<Result<Vec<VecDeque<CompressedPage>>>>()?;

        let row_group = DynIter::new(
            columns
                .into_iter()
                .map(|column| Ok(DynStreamingIterator::new(Bla::new(column)))),
        );
        Ok(row_group)
    });

    // Create a new empty file
    let mut file = std::fs::File::create(path)?;

    // Write the file.
    let _file_size = write_file(
        &mut file,
        row_groups,
        batches[0].schema(),
        parquet_schema,
        options,
        None,
    )?;

    Ok(())
}

fn create_batch(size: usize) -> Result<RecordBatch> {
    let c1: Int32Array = (0..size)
        .map(|x| if x % 9 == 0 { None } else { Some(x as i32) })
        .collect();
    let c2: Utf8Array<i32> = (0..size)
        .map(|x| {
            if x % 8 == 0 {
                None
            } else {
                Some(x.to_string())
            }
        })
        .collect();

    RecordBatch::try_from_iter([
        ("c1", Arc::new(c1) as Arc<dyn Array>),
        ("c2", Arc::new(c2) as Arc<dyn Array>),
    ])
}

fn main() -> Result<()> {
    let batch = create_batch(5_000_000)?;

    parallel_write("example.parquet", &[batch.clone(), batch])
}
