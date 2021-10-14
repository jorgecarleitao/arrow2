use std::fs::File;
use std::iter::once;

use arrow2::error::ArrowError;
use arrow2::io::parquet::write::to_parquet_schema;
use arrow2::{
    array::{Array, Int32Array},
    datatypes::{Field, Schema},
    error::Result,
    io::parquet::write::{
        array_to_pages, write_file, Compression, Compressor, DynIter, DynStreamingIterator,
        Encoding, FallibleStreamingIterator, Version, WriteOptions,
    },
};

fn write_single_array(path: &str, array: &dyn Array, field: Field) -> Result<()> {
    let schema = Schema::new(vec![field]);

    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V2,
    };
    let encoding = Encoding::Plain;

    // map arrow fields to parquet fields
    let parquet_schema = to_parquet_schema(&schema)?;

    let descriptor = parquet_schema.columns()[0].clone();

    // Declare the row group iterator. This must be an iterator of iterators of streaming iterators
    // * first iterator over row groups
    let row_groups = once(Result::Ok(DynIter::new(
        // * second iterator over column chunks (we assume no struct arrays -> `once` column)
        once(
            // * third iterator over (compressed) pages; dictionary encoding may lead to multiple pages per array.
            array_to_pages(array, descriptor, options, encoding).map(move |pages| {
                let encoded_pages = DynIter::new(pages.map(|x| Ok(x?)));
                let compressed_pages = Compressor::new(encoded_pages, options.compression, vec![])
                    .map_err(ArrowError::from);
                DynStreamingIterator::new(compressed_pages)
            }),
        ),
    )));

    // Create a new empty file
    let mut file = File::create(path)?;

    // Write the file. Note that, at present, any error results in a corrupted file.
    let _ = write_file(
        &mut file,
        row_groups,
        &schema,
        parquet_schema,
        options,
        None,
    )?;
    Ok(())
}

fn main() -> Result<()> {
    let array = Int32Array::from(&[
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ]);
    let field = Field::new("c1", array.data_type().clone(), true);
    write_single_array("test.parquet", &array, field)
}
