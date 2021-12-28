use std::fs::File;
use std::sync::Arc;

use arrow2::{
    array::{Array, Int32Array},
    columns::Columns,
    datatypes::{Field, Schema},
    error::Result,
    io::parquet::write::{
        write_file, Compression, Encoding, RowGroupIterator, Version, WriteOptions,
    },
};

fn write_batch(path: &str, schema: Schema, columns: Columns<Arc<dyn Array>>) -> Result<()> {
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V2,
    };

    let iter = vec![Ok(columns)];

    let row_groups =
        RowGroupIterator::try_new(iter.into_iter(), &schema, options, vec![Encoding::Plain])?;

    // Create a new empty file
    let mut file = File::create(path)?;

    // Write the file. Note that, at present, any error results in a corrupted file.
    let parquet_schema = row_groups.parquet_schema().clone();
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
    let schema = Schema::new(vec![field]);
    let columns = Columns::new(vec![Arc::new(array) as Arc<dyn Array>]);

    write_batch("test.parquet", schema, columns)
}
