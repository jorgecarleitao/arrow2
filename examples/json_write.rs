use std::fs::File;
use std::sync::Arc;

use arrow2::{
    array::{Array, Int32Array},
    chunk::Chunk,
    error::Result,
    io::json::write,
};

fn write_batches(path: &str, names: Vec<String>, batches: &[Chunk<Arc<dyn Array>>]) -> Result<()> {
    let mut writer = File::create(path)?;
    let format = write::Format::Json;

    let batches = batches.iter().cloned().map(Ok);

    // Advancing this iterator serializes the next batch to its internal buffer (i.e. CPU-bounded)
    let blocks = write::Serializer::new(batches, names, vec![], format);

    // the operation of writing is IO-bounded.
    write::write(&mut writer, format, blocks)?;

    Ok(())
}

fn main() -> Result<()> {
    let array = Arc::new(Int32Array::from(&[
        Some(0),
        None,
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ])) as Arc<dyn Array>;

    write_batches(
        "example.json",
        vec!["c1".to_string()],
        &[Chunk::new(vec![array.clone()]), Chunk::new(vec![array])],
    )
}
