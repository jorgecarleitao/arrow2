/// Example demonstrating how to write to parquet in parallel.
use std::sync::Arc;

use rayon::prelude::*;

use arrow2::{
    array::*, datatypes::PhysicalType, error::Result, io::parquet::write::*,
    record_batch::RecordBatch,
};

fn parallel_write(path: &str, batch: &RecordBatch) -> Result<()> {
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V2,
    };
    let encodings = batch
        .schema()
        .fields()
        .iter()
        .map(|field| match field.data_type().to_physical_type() {
            // let's be fancy and use delta-encoding for binary fields
            PhysicalType::Binary
            | PhysicalType::LargeBinary
            | PhysicalType::Utf8
            | PhysicalType::LargeUtf8 => Encoding::DeltaLengthByteArray,
            // dictionaries are kept dict-encoded
            PhysicalType::Dictionary(_) => Encoding::RleDictionary,
            // remaining is plain
            _ => Encoding::Plain,
        })
        .collect::<Vec<_>>();

    let parquet_schema = to_parquet_schema(batch.schema())?;

    // write batch to pages; parallelize with rayon
    let columns = batch
        .columns()
        .par_iter()
        .zip(parquet_schema.columns().to_vec().into_par_iter())
        .zip(encodings.into_par_iter())
        .map(|((array, descriptor), encoding)| {
            let array = array.clone();

            // create encoded and compressed pages this column
            array_to_pages(array, descriptor, options, encoding)
                .unwrap()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    // create the iterator over groups (one in this case)
    // (for more batches, create the iterator from them here)
    let row_groups = std::iter::once(Result::Ok(DynIter::new(
        columns
            .into_iter()
            .map(|column| Ok(DynIter::new(column.into_iter()))),
    )));

    // Create a new empty file
    let mut file = std::fs::File::create(path)?;

    // Write the file.
    let _file_size = write_file(
        &mut file,
        row_groups,
        batch.schema(),
        parquet_schema,
        options,
        None,
    )?;

    Ok(())
}

fn main() -> Result<()> {
    let c1 = Int32Array::from(&[Some(0), None, None, Some(2), Some(3)]);
    let c2 = Utf8Array::<i32>::from(&[Some("1"), None, None, Some("2"), Some("3")]);

    // and a dict array
    let data = [Some("hello"), Some("bye"), None, Some("hello"), Some("bye")];
    let mut a = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    a.try_extend(data)?;
    let c3: DictionaryArray<i32> = a.into();

    let batch = RecordBatch::try_from_iter([
        ("c1", Arc::new(c1) as Arc<dyn Array>),
        ("c2", Arc::new(c2) as Arc<dyn Array>),
        ("c3", Arc::new(c3) as Arc<dyn Array>),
    ])?;

    parallel_write("example.parquet", &batch)
}
