use std::fs::File;
use std::sync::Arc;

use arrow2::array::{Array, Int32Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::Result;
use arrow2::io::ipc::write;

fn write_batches(path: &str, schema: &Schema, columns: &[Chunk<Arc<dyn Array>>]) -> Result<()> {
    let file = File::create(path)?;

    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::try_new(file, schema, None, options)?;

    for columns in columns {
        writer.write(columns, None)?
    }
    writer.finish()
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    // create a batch
    let schema = Schema::from(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]);

    let a = Int32Array::from_slice(&[1, 2, 3, 4, 5]);
    let b = Utf8Array::<i32>::from_slice(&["a", "b", "c", "d", "e"]);

    let batch = Chunk::try_new(vec![Arc::new(a) as Arc<dyn Array>, Arc::new(b)])?;

    // write it
    write_batches(file_path, &schema, &[batch])?;
    Ok(())
}
