use std::fs::File;
use std::sync::Arc;

use arrow2::{
    array::Int32Array,
    datatypes::{Field, Schema},
    error::Result,
    io::json::write,
    record_batch::RecordBatch,
};

fn write_batches(path: &str, batches: &[RecordBatch]) -> Result<()> {
    let mut writer = File::create(path)?;
    let format = write::JsonArray::default();

    let batches = batches.iter().cloned().map(Ok);

    // Advancing this iterator serializes the next batch to its internal buffer (i.e. CPU-bounded)
    let blocks = write::Serializer::new(batches, vec![], format);

    // the operation of writing is IO-bounded.
    write::write(&mut writer, format, blocks)?;

    Ok(())
}

fn main() -> Result<()> {
    let array = Int32Array::from(&[Some(0), None, Some(2), Some(3), Some(4), Some(5), Some(6)]);
    let field = Field::new("c1", array.data_type().clone(), true);
    let schema = Schema::new(vec![field]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])?;

    write_batches("example.json", &[batch.clone(), batch])
}
