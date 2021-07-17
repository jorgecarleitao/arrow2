use std::fs::File;
use std::sync::Arc;

use arrow2::array::{Int32Array, Utf8Array};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::Result;
use arrow2::io::ipc::write;
use arrow2::record_batch::RecordBatch;

fn write_batches(path: &str, schema: &Schema, batches: &[RecordBatch]) -> Result<()> {
    let mut file = File::create(path)?;

    let mut writer = write::FileWriter::try_new(&mut file, schema)?;

    for batch in batches {
        writer.write(batch)?
    }
    writer.finish()
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    // create a batch
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]);

    let a = Int32Array::from_slice(&[1, 2, 3, 4, 5]);
    let b = Utf8Array::<i32>::from_slice(&["a", "b", "c", "d", "e"]);

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a), Arc::new(b)])?;

    // write it
    write_batches(file_path, &schema, &[batch])?;
    Ok(())
}
