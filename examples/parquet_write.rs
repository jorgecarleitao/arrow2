use std::fs::File;
use std::iter::once;

use arrow2::{
    array::{Array, Int32Array},
    datatypes::Field,
    error::Result,
    io::parquet::write::{
        array_to_page, to_parquet_type, write_file, CompressionCodec, SchemaDescriptor,
    },
};

fn write_single_array(path: &str, array: &dyn Array, field: &Field) -> Result<()> {
    // Create a new empty file
    let mut file = File::create(path)?;

    // convert arrows' logical type into a parquet type.
    let parquet_type = to_parquet_type(&field)?;

    let compression = CompressionCodec::Uncompressed;

    // Declare the row groups iterator. This must be an iterator of iterators of iterators:
    // * first iterator of row groups
    // * second iterator of column chunks
    // * third iterator of pages
    // an array can always be divided in multiple pages via `.slice(offset, length)` (`O(1)`).
    // All column chunks within a row group MUST have the same length.
    let row_groups = once(Result::Ok(once(Result::Ok(once(array_to_page(
        array,
        &parquet_type,
        compression,
    ))))));

    // Create a parquet schema descriptor with all parquet types.
    let schema = SchemaDescriptor::new("root".to_string(), vec![parquet_type]);

    // Write the file. Note that, at present, any error results in a corrupted file.
    Ok(write_file(&mut file, schema, compression, row_groups)?)
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
    write_single_array("test.parquet", &array, &field)
}
