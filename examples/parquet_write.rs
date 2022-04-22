use std::fs::File;
use std::sync::Arc;

use arrow2::{
    array::{Array, Int32Array},
    chunk::Chunk,
    datatypes::{Field, Schema},
    error::Result,
    io::parquet::write::{
        CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions,
    },
};

fn write_batch(path: &str, schema: Schema, columns: Chunk<Arc<dyn Array>>) -> Result<()> {
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
    };

    let iter = vec![Ok(columns)];

    let row_groups =
        RowGroupIterator::try_new(iter.into_iter(), &schema, options, vec![Encoding::Plain])?;

    // Create a new empty file
    let file = File::create(path)?;

    let mut writer = FileWriter::try_new(file, schema, options)?;

    writer.start()?;
    for group in row_groups {
        writer.write(group?)?;
    }
    let _size = writer.end(None)?;
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
    let schema = Schema::from(vec![field]);
    let columns = Chunk::new(vec![Arc::new(array) as Arc<dyn Array>]);

    write_batch("test.parquet", schema, columns)
}
